# Vendored Apache Cassandra CQL examples

`doc/modules/cassandra/examples/CQL/` holds the official CQL example snippets from
Apache Cassandra, used by `tests/drivers/python/test_cassandra_cql_examples.py` to
validate ferrosa's wire-level CQL compatibility by executing each statement
against a live node.

- **Source:** https://github.com/apache/cassandra (branch `cassandra-5.0`)
- **Commit:** ffe7f761b8a0bc170d7cacedc53c5e0607847d25
- **License:** Apache License 2.0 — https://github.com/apache/cassandra/blob/trunk/LICENSE.txt

Files are unmodified upstream examples vendored for offline/reproducible testing.
Refresh via a sparse checkout of that path.
