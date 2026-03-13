# ADR-006: CQL Architecture Decisions

> Date: 2026-03-12
> Status: Accepted

## Context

`ferrosa-cql` implements CQL native protocol v5 as the client-facing interface to Ferrosa. Several architectural decisions shape its design, each with alternatives considered.

## Decisions

### 1. Lock-Free Concurrency Model

**Decision**: Use `ArcSwap<SchemaSnapshot>` for schema lookups, `moka` concurrent cache for prepared statements, and `Arc<StorageEngine>` for storage access. No `Mutex` on any hot path.

**Alternatives considered**:

- `RwLock<SchemaSnapshot>`: Simpler but creates contention under high query rates. Reader starvation possible under frequent schema changes.
- Per-connection schema copy: Eliminates sharing but wastes memory and makes schema propagation complex.

**Rationale**: Every query must access the schema and potentially the prepared cache. With thousands of concurrent connections, even a `RwLock` creates measurable contention. `ArcSwap::load()` is a single atomic load; `moka` uses W-TinyLFU with lock-free reads. This matches the Cassandra JVM approach (concurrent data structures on the hot path).

### 2. Hand-Written Recursive Descent Parser

**Decision**: Implement CQL parsing as a hand-written recursive descent parser with one function per grammar rule. No parser generator.

**Alternatives considered**:

- `nom`: Combinator-based. Good for binary formats but produces opaque error messages for text grammars. CQL error messages need byte offsets and snippets.
- `pest`/`lalrpop`: Grammar-file-based generators. Extra build step, harder to debug, grammar files diverge from code over time.
- `tree-sitter`: Designed for editors, not database query parsing. Overhead for incremental parsing not needed.

**Rationale**: CQL grammar is LL(2) — at most 2-token lookahead (`CREATE TABLE` vs `CREATE KEYSPACE`). Single-pass, O(n), no backtracking. Hand-written parsers produce the best error messages (Cassandra itself uses ANTLR but wraps it heavily for error reporting). Each grammar rule is a named function, making the parser self-documenting and easy to extend.

### 3. Reject ALLOW FILTERING

**Decision**: `ALLOW FILTERING` queries return ERROR(Invalid) in the initial implementation. Full table scans are not supported until secondary index work begins.

**Alternatives considered**:

- Accept and execute: Would work for single-node but creates a performance trap. Users expect `ALLOW FILTERING` to be slow but functional; without secondary indices, it's a full SSTable scan.
- Accept with warning: Confusing — the query "works" but may return incomplete results if data is spread across nodes.

**Rationale**: Cassandra's `ALLOW FILTERING` is the single most common source of production incidents. Rejecting it early forces users to design proper data models. Support will be added alongside secondary indices where filtered queries can be executed efficiently.

## Consequences

- Lock-free design requires careful attention to memory ordering but eliminates contention bottlenecks
- Hand-written parser requires more initial code but is easier to maintain and produces better errors
- `ALLOW FILTERING` rejection may surprise users migrating from Cassandra, but prevents performance traps
