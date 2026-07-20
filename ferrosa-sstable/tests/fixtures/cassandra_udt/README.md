# cassandra_udt fixture

Real Cassandra 5.0 (`nb-big`, uncompressed) `Data.db` for a NON-FROZEN UDT column:

```
CREATE TYPE test.addr (street text, zip int);
CREATE TABLE test.udt_tbl (pk text PRIMARY KEY, a addr) WITH compression={'enabled':'false'};
INSERT INTO test.udt_tbl (pk, a) VALUES ('row1', {street:'main', zip:12345});
```

A non-frozen UDT is a COMPLEX column: each field is a cell whose cell-path is a
2-byte big-endian field position (`uvint(2)+[00 00]` = field 0, `uvint(2)+[00 01]`
= field 1), value length-prefixed. A plain INSERT sets HAS_COMPLEX_DELETION.
Used by cassandra_compat.rs::read_real_cassandra_udt.
