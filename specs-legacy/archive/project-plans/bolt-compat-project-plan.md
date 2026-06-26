# Bolt Compatibility Testing — Project Plan

> Source: /blueprint on bolt-compat-testing.md

## Sprint 1: Infrastructure + Wire Protocol (S)

| Task | Description | Success Criteria | Size |
|------|-------------|-----------------|------|
| 1.1 | Add `neo4j>=5.0` to `tests/drivers/python/requirements.txt` | `pip install -r requirements.txt` succeeds | S |
| 1.2 | Create `tests/drivers/python/test_bolt_compat.py` with fixtures | `pytest test_bolt_compat.py --co` lists all tests | S |
| 1.3 | Schema setup fixture: CQL DDL for social graph | Tables + extensions created via cassandra-driver | S |
| 1.4 | Seed data fixture: insert vertices and edges via CQL | All 5 persons, 2 companies, 5 KNOWS, 4 WORKS_AT edges present | S |
| 1.5 | Wire protocol tests (Cat 1): connect, auth, RUN/PULL, reset, close | 8 tests pass | M |

## Sprint 2: Cypher Queries + Data Types (M)

| Task | Description | Success Criteria | Size |
|------|-------------|-----------------|------|
| 2.1 | Cypher query tests (Cat 2): MATCH patterns, CREATE, SET, DELETE | 18 tests pass | M |
| 2.2 | Data type fidelity tests (Cat 3): int, float, string, bool, null, list | 9 tests pass | S |
| 2.3 | Aggregation tests (Cat 4): count, sum, avg, min, max, collect, group by | 7 tests pass | S |
| 2.4 | Error handling tests (Cat 5): syntax, unknown label, type mismatch | 5 tests pass | S |

## Sprint 3: Index Verification + CI (S)

| Task | Description | Success Criteria | Size |
|------|-------------|-----------------|------|
| 3.1 | Index verification tests (Cat 6): verify adjacency index usage via stats | 5 tests pass, stats show adjacency reads | S |
| 3.2 | Add Bolt tests to `tests/drivers/run-all.sh` | `run-all.sh` includes Bolt test stage | S |
| 3.3 | Document in CLAUDE.md | Test running instructions added | S |

## Total: ~52 tests across 6 categories
