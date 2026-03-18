USE cql_compat;

-- ============================================================
-- Basic SELECT — all columns and column subset
-- ============================================================
SELECT * FROM all_types WHERE id = 1;
SELECT col_text, col_int, col_bigint FROM all_types WHERE id = 1;

-- ============================================================
-- NULL check — unset columns return null
-- ============================================================
SELECT * FROM all_types WHERE id = 2;

-- ============================================================
-- Negative boundary values
-- ============================================================
SELECT col_bigint, col_int, col_float, col_double FROM all_types WHERE id = 4;

-- ============================================================
-- Zero values and false boolean
-- ============================================================
SELECT col_bigint, col_int, col_float, col_double, col_boolean FROM all_types WHERE id = 5;

-- ============================================================
-- Collection reads
-- ============================================================
SELECT tags, scores, props FROM collection_types WHERE id = 1;

-- Empty collections
SELECT tags, scores, props FROM collection_types WHERE id = 2;

-- Nested frozen collections
SELECT nested_list, nested_map FROM collection_types WHERE id = 3;

-- ============================================================
-- Composite partition key queries
-- ============================================================
SELECT * FROM composite_pk WHERE tenant = 'acme' AND region = 'us-east';

-- ORDER BY with clustering column
SELECT * FROM composite_pk WHERE tenant = 'acme' AND region = 'us-east' ORDER BY event_time DESC;

-- LIMIT
SELECT * FROM composite_pk WHERE tenant = 'acme' AND region = 'us-east' LIMIT 1;

-- ============================================================
-- Static columns
-- ============================================================
SELECT * FROM static_cols WHERE group_id = 1;

-- DISTINCT on partition key with static column
SELECT DISTINCT group_id, group_name FROM static_cols;

-- ============================================================
-- TTL verification
-- ============================================================

-- Permanent row — always present
SELECT * FROM ttl_test WHERE id = 1;

-- USING TIMESTAMP — last-write-wins (new_data at ts 2000000 beats old_data at 1000000)
SELECT * FROM ttl_test WHERE id = 3;

-- ============================================================
-- LWT result — second INSERT IF NOT EXISTS was a no-op
-- ============================================================
SELECT * FROM lwt_test WHERE id = 1;

-- ============================================================
-- Batch results
-- ============================================================
SELECT * FROM batch_test WHERE id = 1;
SELECT * FROM batch_test WHERE id = 4;

-- ============================================================
-- Counter reads
-- ============================================================
SELECT * FROM counters WHERE id = 'homepage';
SELECT * FROM counters WHERE id = 'about';

-- ============================================================
-- DELETE operations
-- ============================================================

-- Row delete — remove a single clustering row
DELETE FROM delete_test WHERE pk = 1 AND ck = 2;
SELECT * FROM delete_test WHERE pk = 1;

-- Partition delete — remove all rows in a partition
DELETE FROM delete_test WHERE pk = 2;
SELECT * FROM delete_test WHERE pk = 2;

-- ============================================================
-- Count aggregation
-- ============================================================
SELECT COUNT(*) FROM all_types;
SELECT COUNT(*) FROM batch_test;

-- ============================================================
-- Secondary index queries
-- ============================================================
SELECT * FROM all_types WHERE col_text = 'updated';
SELECT * FROM all_types WHERE col_int = 43;

-- ============================================================
-- UDT queries
-- ============================================================
SELECT * FROM udt_test WHERE id = 1;

-- ============================================================
-- Tuple queries
-- ============================================================
SELECT * FROM tuple_test WHERE id = 1;
SELECT coords, labeled FROM tuple_test WHERE id = 2;

-- ============================================================
-- UPDATE with IF condition (LWT conditional update)
-- ============================================================
UPDATE lwt_test SET data = 'modified' WHERE id = 1 IF version = 1;
SELECT * FROM lwt_test WHERE id = 1;

-- ============================================================
-- DELETE with IF EXISTS (LWT conditional delete)
-- ============================================================
DELETE FROM lwt_test WHERE id = 1 IF EXISTS;
SELECT * FROM lwt_test WHERE id = 1;

-- ============================================================
-- Token function — range scan by token
-- ============================================================
SELECT * FROM all_types WHERE token(id) > token(3);

-- ============================================================
-- WRITETIME function
-- ============================================================
SELECT col_text, WRITETIME(col_text) FROM all_types WHERE id = 1;

-- ============================================================
-- IN clause on partition key
-- ============================================================
SELECT * FROM all_types WHERE id IN (1, 2, 3);

-- ============================================================
-- ALLOW FILTERING
-- ============================================================
SELECT * FROM all_types WHERE col_boolean = true ALLOW FILTERING;

-- ============================================================
-- TTL function — check remaining TTL on a column
-- ============================================================
SELECT val, TTL(val) FROM ttl_test WHERE id = 1;

-- ============================================================
-- Cleanup: DROP TYPE, DROP TABLE, DROP INDEX, DROP KEYSPACE
-- ============================================================
DROP INDEX IF EXISTS cql_compat.all_types_col_text_idx;
DROP INDEX IF EXISTS cql_compat.all_types_col_int_idx;
DROP INDEX IF EXISTS cql_compat.composite_pk_data_idx;
DROP TABLE IF EXISTS udt_test;
DROP TABLE IF EXISTS tuple_test;
DROP TABLE IF EXISTS all_types;
DROP TABLE IF EXISTS collection_types;
DROP TABLE IF EXISTS composite_pk;
DROP TABLE IF EXISTS static_cols;
DROP TABLE IF EXISTS ttl_test;
DROP TABLE IF EXISTS lwt_test;
DROP TABLE IF EXISTS batch_test;
DROP TABLE IF EXISTS counters;
DROP TABLE IF EXISTS delete_test;
DROP TYPE IF EXISTS phone_number;
DROP TYPE IF EXISTS address;
DROP KEYSPACE IF EXISTS cql_compat;
