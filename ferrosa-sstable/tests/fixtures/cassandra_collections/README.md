# cassandra_collections fixture

Real Cassandra 5.0 (`nb-big` format, uncompressed) SSTable `Data.db` for:

```
CREATE TABLE test.collections (pk text PRIMARY KEY, l list<int>, s set<text>, m map<text,int>)
  WITH compression = {'enabled':'false'};
INSERT INTO test.collections (pk,l,s,m)
  VALUES ('row1',[10,20,30],{'a','b','c'},{'k1':1,'k2':2});
```

Storage column order is alphabetical: `l`, `m`, `s`. A plain collection INSERT
sets the row's `HAS_COMPLEX_DELETION` flag (each collection column carries a
complex `DeletionTime`). Used by `cassandra_compat.rs::read_real_cassandra_collections`
to prove ferrosa parses real Cassandra complex-column cells.
