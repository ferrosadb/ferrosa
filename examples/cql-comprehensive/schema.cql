CREATE KEYSPACE IF NOT EXISTS cql_compat WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE cql_compat;

-- ============================================================
-- Table 1: Every CQL scalar type
-- ============================================================
CREATE TABLE IF NOT EXISTS all_types (
    id           int PRIMARY KEY,
    col_text     text,
    col_ascii    ascii,
    col_bigint   bigint,
    col_int      int,
    col_smallint smallint,
    col_tinyint  tinyint,
    col_boolean  boolean,
    col_float    float,
    col_double   double,
    col_decimal  decimal,
    col_blob     blob,
    col_uuid     uuid,
    col_timeuuid timeuuid,
    col_timestamp timestamp,
    col_date     date,
    col_time     time,
    col_inet     inet,
    col_varint   varint
);

-- ============================================================
-- Table 2: Collection types — list, set, map, nested frozen
-- ============================================================
CREATE TABLE IF NOT EXISTS collection_types (
    id         int PRIMARY KEY,
    tags       list<text>,
    scores     set<int>,
    props      map<text, text>,
    nested_list list<frozen<list<int>>>,
    nested_map  map<text, frozen<map<text, int>>>
);

-- ============================================================
-- Table 3: Composite partition key with clustering order
-- ============================================================
CREATE TABLE IF NOT EXISTS composite_pk (
    tenant     text,
    region     text,
    event_id   uuid,
    event_time timestamp,
    data       text,
    PRIMARY KEY ((tenant, region), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);

-- ============================================================
-- Table 4: Static columns
-- ============================================================
CREATE TABLE IF NOT EXISTS static_cols (
    group_id   int,
    item_id    int,
    group_name text STATIC,
    item_name  text,
    PRIMARY KEY (group_id, item_id)
);

-- ============================================================
-- Table 5: TTL and TIMESTAMP tests
-- ============================================================
CREATE TABLE IF NOT EXISTS ttl_test (
    id  int PRIMARY KEY,
    val text
);

-- ============================================================
-- Table 6: Lightweight transaction tests
-- ============================================================
CREATE TABLE IF NOT EXISTS lwt_test (
    id      int PRIMARY KEY,
    version int,
    data    text
);

-- ============================================================
-- Table 7: Batch tests
-- ============================================================
CREATE TABLE IF NOT EXISTS batch_test (
    id  int PRIMARY KEY,
    val text
);

-- ============================================================
-- Table 8: Counter type
-- ============================================================
CREATE TABLE IF NOT EXISTS counters (
    id              text PRIMARY KEY,
    page_views      counter,
    unique_visitors counter
);

-- ============================================================
-- Table 9: DELETE tests — partition and range deletes
-- ============================================================
CREATE TABLE IF NOT EXISTS delete_test (
    pk  int,
    ck  int,
    val text,
    PRIMARY KEY (pk, ck)
);

-- ============================================================
-- User-Defined Types
-- ============================================================
CREATE TYPE IF NOT EXISTS address (
    street  text,
    city    text,
    state   text,
    zip     text,
    country text
);

CREATE TYPE IF NOT EXISTS phone_number (
    country_code int,
    number       text,
    phone_type   text
);

-- ============================================================
-- Table 10: UDT columns
-- ============================================================
CREATE TABLE IF NOT EXISTS udt_test (
    id           int PRIMARY KEY,
    home_address frozen<address>,
    phones       list<frozen<phone_number>>,
    work_address frozen<address>
);

-- ============================================================
-- Table 11: Tuple columns
-- ============================================================
CREATE TABLE IF NOT EXISTS tuple_test (
    id      int PRIMARY KEY,
    coords  frozen<tuple<double, double>>,
    labeled frozen<tuple<text, int, boolean>>
);

-- ============================================================
-- Secondary indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS ON all_types (col_text);
CREATE INDEX IF NOT EXISTS ON all_types (col_int);
CREATE INDEX IF NOT EXISTS ON composite_pk (data);

-- ============================================================
-- ALTER TABLE — add columns after creation
-- ============================================================
ALTER TABLE all_types ADD extra_col text;
ALTER TABLE all_types ADD another_col int;
