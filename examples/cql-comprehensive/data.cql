USE cql_compat;

-- ============================================================
-- All scalar types — positive values
-- ============================================================
INSERT INTO all_types (id, col_text, col_ascii, col_bigint, col_int, col_smallint, col_tinyint,
    col_boolean, col_float, col_double, col_decimal, col_blob, col_uuid, col_timestamp,
    col_date, col_time, col_inet)
VALUES (1, 'hello', 'ascii_val', 9223372036854775807, 42, 32767, 127,
    true, 3.14, 2.718281828459045, 99.99, 0xdeadbeef,
    550e8400-e29b-41d4-a716-446655440000, '2024-06-15T10:30:00Z',
    '2024-06-15', '10:30:00', '192.168.1.1');

-- ============================================================
-- Null value
-- ============================================================
INSERT INTO all_types (id, col_text) VALUES (2, null);

-- ============================================================
-- Empty string (distinct from null)
-- ============================================================
INSERT INTO all_types (id, col_text) VALUES (3, '');

-- ============================================================
-- Negative boundary values
-- ============================================================
INSERT INTO all_types (id, col_bigint, col_int, col_float, col_double)
VALUES (4, -9223372036854775807, -2147483647, -3.14, -2.718281828459045);

-- ============================================================
-- Zero values and false boolean
-- ============================================================
INSERT INTO all_types (id, col_bigint, col_int, col_float, col_double, col_boolean)
VALUES (5, 0, 0, 0.0, 0.0, false);

-- ============================================================
-- UUID function call as value
-- ============================================================
INSERT INTO all_types (id, col_uuid) VALUES (6, uuid());

-- ============================================================
-- Collection types — populated
-- ============================================================
INSERT INTO collection_types (id, tags, scores, props)
VALUES (1, ['alpha', 'beta', 'gamma'], {10, 20, 30}, {'color': 'red', 'size': 'large'});

-- ============================================================
-- Collection types — empty collections
-- ============================================================
INSERT INTO collection_types (id, tags, scores, props)
-- Note: empty {} is ambiguous between set and map in CQL.
-- Use explicit null for empty collections, or insert non-empty then clear.
VALUES (2, [], null, null);

-- ============================================================
-- Nested frozen collections
-- ============================================================
INSERT INTO collection_types (id, nested_list, nested_map)
VALUES (3, [[1, 2, 3], [4, 5]], {'totals': {'a': 10, 'b': 20}});

-- ============================================================
-- Composite partition key — multiple partitions
-- ============================================================
INSERT INTO composite_pk (tenant, region, event_id, event_time, data)
VALUES ('acme', 'us-east', uuid(), '2024-06-15T10:00:00Z', 'event_1');
INSERT INTO composite_pk (tenant, region, event_id, event_time, data)
VALUES ('acme', 'us-east', uuid(), '2024-06-15T11:00:00Z', 'event_2');
INSERT INTO composite_pk (tenant, region, event_id, event_time, data)
VALUES ('acme', 'us-west', uuid(), '2024-06-15T12:00:00Z', 'event_3');
INSERT INTO composite_pk (tenant, region, event_id, event_time, data)
VALUES ('globex', 'eu-west', uuid(), '2024-06-15T13:00:00Z', 'event_4');

-- ============================================================
-- Static columns — group_name set once, shared across rows
-- ============================================================
INSERT INTO static_cols (group_id, item_id, group_name, item_name) VALUES (1, 1, 'Group A', 'Item 1');
INSERT INTO static_cols (group_id, item_id, item_name) VALUES (1, 2, 'Item 2');
INSERT INTO static_cols (group_id, item_id, item_name) VALUES (1, 3, 'Item 3');
INSERT INTO static_cols (group_id, item_id, group_name, item_name) VALUES (2, 1, 'Group B', 'Item X');

-- ============================================================
-- TTL — permanent and expiring rows
-- ============================================================
INSERT INTO ttl_test (id, val) VALUES (1, 'permanent');
INSERT INTO ttl_test (id, val) VALUES (2, 'expires_soon') USING TTL 1;

-- ============================================================
-- USING TIMESTAMP — explicit write timestamps
-- ============================================================
INSERT INTO ttl_test (id, val) VALUES (3, 'old_data') USING TIMESTAMP 1000000;
INSERT INTO ttl_test (id, val) VALUES (3, 'new_data') USING TIMESTAMP 2000000;

-- ============================================================
-- LWT: INSERT IF NOT EXISTS — first succeeds, second is a no-op
-- ============================================================
INSERT INTO lwt_test (id, version, data) VALUES (1, 1, 'original') IF NOT EXISTS;
INSERT INTO lwt_test (id, version, data) VALUES (1, 2, 'duplicate') IF NOT EXISTS;

-- ============================================================
-- Logged batch
-- ============================================================
BEGIN BATCH
    INSERT INTO batch_test (id, val) VALUES (1, 'batch_1');
    INSERT INTO batch_test (id, val) VALUES (2, 'batch_2');
    INSERT INTO batch_test (id, val) VALUES (3, 'batch_3');
APPLY BATCH;

-- ============================================================
-- Unlogged batch
-- ============================================================
BEGIN UNLOGGED BATCH
    INSERT INTO batch_test (id, val) VALUES (4, 'unlogged_1');
    INSERT INTO batch_test (id, val) VALUES (5, 'unlogged_2');
APPLY BATCH;

-- ============================================================
-- Counter updates — increment operations
-- ============================================================
UPDATE counters SET page_views = page_views + 100, unique_visitors = unique_visitors + 42 WHERE id = 'homepage';
UPDATE counters SET page_views = page_views + 50 WHERE id = 'homepage';
UPDATE counters SET page_views = page_views + 1 WHERE id = 'about';

-- ============================================================
-- DELETE test data
-- ============================================================
INSERT INTO delete_test (pk, ck, val) VALUES (1, 1, 'a');
INSERT INTO delete_test (pk, ck, val) VALUES (1, 2, 'b');
INSERT INTO delete_test (pk, ck, val) VALUES (1, 3, 'c');
INSERT INTO delete_test (pk, ck, val) VALUES (2, 1, 'x');
INSERT INTO delete_test (pk, ck, val) VALUES (2, 2, 'y');

-- ============================================================
-- UPDATE — simple column updates
-- ============================================================
UPDATE all_types SET col_text = 'updated', extra_col = 'added' WHERE id = 1;
UPDATE all_types SET col_int = col_int + 1 WHERE id = 1;

-- ============================================================
-- Collection updates — append, remove, element access
-- ============================================================

-- List append and remove
UPDATE collection_types SET tags = tags + ['delta'] WHERE id = 1;
UPDATE collection_types SET tags = tags - ['alpha'] WHERE id = 1;

-- Set add and remove
UPDATE collection_types SET scores = scores + {40, 50} WHERE id = 1;
UPDATE collection_types SET scores = scores - {10} WHERE id = 1;

-- Map put and remove
UPDATE collection_types SET props['weight'] = 'heavy' WHERE id = 1;
UPDATE collection_types SET props = props - {'color'} WHERE id = 1;

-- ============================================================
-- UDT insert — nested user-defined type values
-- ============================================================
INSERT INTO udt_test (id, home_address, phones, work_address)
VALUES (1,
    {street: '123 Main St', city: 'Springfield', state: 'IL', zip: '62704', country: 'US'},
    [{country_code: 1, number: '555-0100', phone_type: 'mobile'},
     {country_code: 1, number: '555-0200', phone_type: 'work'}],
    {street: '456 Oak Ave', city: 'Chicago', state: 'IL', zip: '60601', country: 'US'}
);

-- ============================================================
-- Tuple insert
-- ============================================================
INSERT INTO tuple_test (id, coords, labeled)
VALUES (1, (37.7749, -122.4194), ('active', 42, true));
INSERT INTO tuple_test (id, coords, labeled)
VALUES (2, (51.5074, -0.1278), ('inactive', 0, false));
