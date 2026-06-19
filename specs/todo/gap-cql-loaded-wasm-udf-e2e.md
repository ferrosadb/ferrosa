---
type: gap
priority: P0
status: in_progress
created: 2026-05-20
updated: 2026-05-20
---

# CQL-loaded WASM UDFs must be deterministic and executable cluster-wide

## Why

`CREATE FUNCTION ... LANGUAGE wasm AS '<hex>'` is partially implemented. The
parser, router, schema metadata, and Wasmtime compile path exist, and query-time
scalar UDF evaluation is wired for SELECT projections. The feature is not yet
complete enough to treat CQL-loaded WASM as production-ready.

Current working pieces:

- `CREATE FUNCTION` and `DROP FUNCTION` parse into AST statements.
- Router rejects non-`wasm` languages, resolves argument and return types,
  hex-decodes `AS '<hex>'`, compiles through `UdfExecutor`, and stores
  `UserFunctionMetadata`.
- `0x` / `0X` prefixes are accepted inside the string body.
- Scalar UDFs in SELECT projections call the WASM executor.
- DDL operations carry function metadata through direct, pair, and cluster DDL
  paths.

Current gaps:

- The accepted source format is inline hex only; examples still imply local
  path/blob loading such as `AS 'percentile_95.wasm'` or
  `system.wasm_binaries`.
- `CREATE OR REPLACE FUNCTION` compiles and invalidates cache state but schema
  replacement is not wired.
- `IF NOT EXISTS` decodes and compiles before duplicate detection.
- The executor registry is keyed by `(keyspace, name)` while schema functions
  are keyed by `(keyspace, name, arg_types)`, so overloads collide.
- Stored function bodies are not recompiled on follower DDL apply, schema
  snapshot install, or process restart.
- There is no successful router-level end-to-end test proving
  `SELECT udf(col)` returns an expected value from a real component exporting
  `invoke`.
- UDFs in `WHERE` are detected for `ALLOW FILTERING` gating but are not
  evaluated as predicates.
- Query-time UDF execution does not appear to check `EXECUTE` permission on the
  function resource.

Current branch progress:

- Parser and AST support inline hex, `AS FILE '<path>'`, and
  `AS URL '<url>' WITH SHA256 = '<digest>'`.
- Router resolves file and URL artifacts to bytes before commit; URL artifacts
  are SHA-256 verified and stored as deterministic hex bytes.
- WASM loading through `CREATE FUNCTION LANGUAGE wasm` now requires a superuser
  role.
- Function metadata and executor cache identity include argument types, so
  overloads do not collide.
- `CREATE OR REPLACE FUNCTION` replaces schema metadata after compiling the new
  component, and `IF NOT EXISTS` returns before decoding or compiling duplicate
  bodies.

Current branch limitations:

- File/URL loading still needs configured artifact policy controls such as URL
  allowlists, redirect limits, and content-size/content-type enforcement.
- Stored metadata does not yet include `wasm_sha256` audit fields.
- Restart/schema-snapshot recompilation and replicated follower execution still
  need end-to-end coverage.
- UDF predicates in `WHERE` remain specified but not implemented.
- Query-time `EXECUTE` permission checks still need to be added.

## Proposed v1 contract

Support inline hex plus admin-only file/URL import. All accepted forms must
normalize to deterministic WebAssembly Component Model bytes plus `sha256`
before schema replication.

Inline hex remains the portable schema form:

```sql
CREATE OR REPLACE FUNCTION ks.to_celsius(f double)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS '0x<hex-encoded-component-bytes>';
```

File and URL loading are admin-only convenience surfaces. They must fetch/read
the artifact on the coordinator, enforce size and content-type limits, compute
`sha256`, compile the component, and replicate the resolved bytes/hash rather
than the original path or URL:

```sql
CREATE OR REPLACE FUNCTION ks.to_celsius(f double)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS FILE '/secure/udf/to_celsius.wasm';

CREATE OR REPLACE FUNCTION ks.to_celsius(f double)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE wasm
AS URL 'https://artifacts.example/internal/to_celsius.wasm'
WITH SHA256 = '<expected-hex-digest>';
```

Only authenticated administrators may load UDF bytes by any mechanism. URL
loading requires an expected SHA-256 digest, TLS verification, redirect limits,
and an allowlist or configured artifact provider.

## Proposed change

1. Align schema and executor identity:
   use `(keyspace, name, arg_types)` for compiled registry entries, pools,
   invalidation, and lookup. Include `wasm_sha256` in metadata for auditability
   and stale-cache detection.
2. Fix DDL ordering:
   check existing function metadata before decode/compile; return immediately
   for `IF NOT EXISTS`; compile replacements into a temporary entry; atomically
   replace schema metadata and executor cache for `OR REPLACE`.
3. Add function recompilation from metadata:
   compile all stored WASM functions after schema snapshot load, follower/pair
   DDL apply, raft snapshot install, and process restart.
4. Add successful execution coverage:
   use a real small Component Model module exporting the expected `invoke`
   function, register it via CQL, insert rows, and verify `SELECT udf(col)`.
5. Enforce permissions:
   loading/replacing/dropping UDFs requires admin privileges, and query-time
   execution checks `EXECUTE` on `Resource::Function` or equivalent during UDF
   planning or evaluation.
6. Implement UDF predicates in `WHERE`:
   allow deterministic scalar UDF calls in WHERE clauses, require
   `ALLOW FILTERING` unless a future planner can prove an index-safe rewrite,
   evaluate them after storage-row retrieval, and reject non-boolean predicate
   results with a clear error.
7. Add file/URL import:
   parse admin-only `AS FILE` and `AS URL ... WITH SHA256`, resolve to bytes on
   the coordinator, compile before commit, store bytes/hash in schema metadata,
   and replicate only deterministic metadata.
8. Update examples and docs:
   align examples with inline hex, `AS FILE`, or `AS URL ... WITH SHA256`;
   document the required Component Model WIT `invoke` ABI.

## Acceptance criteria

- [ ] `CREATE FUNCTION ... LANGUAGE wasm AS '<hex>'` with a real component can
      be called successfully in `SELECT`.
- [ ] Admin-only `AS FILE '<path>'` resolves to bytes/hash and creates the same
      executable function metadata as inline hex.
- [ ] Admin-only `AS URL '<url>' WITH SHA256 = '<digest>'` resolves to
      bytes/hash, verifies the digest, and creates executable function metadata.
- [ ] Non-admin users cannot create, replace, drop, or import WASM UDFs.
- [ ] `CREATE OR REPLACE FUNCTION` changes the result for the same signature.
- [ ] `CREATE FUNCTION IF NOT EXISTS` on an existing signature does not decode,
      compile, replace, or invalidate the existing function.
- [ ] Overloaded functions with the same name and different arg types do not
      collide in the executor cache.
- [ ] Restarting a node with stored function metadata recompiles functions and
      keeps `SELECT udf(col)` working.
- [ ] Replicated DDL makes follower/peer nodes able to execute the function
      after schema apply.
- [ ] `DROP FUNCTION` invalidates only the matching overload.
- [ ] UDF execution requires the correct function-level permission.
- [ ] Deterministic scalar UDF predicates in `WHERE ... ALLOW FILTERING` are
      evaluated correctly.
- [ ] UDF predicates in `WHERE` without `ALLOW FILTERING` are rejected unless a
      later indexed-planner path explicitly supports them.
- [ ] `examples/advanced-aggregation/` and
      `examples/timeseries-rrd/custom-wasm-udf.cql` match the implemented CQL
      source format.

## Verification commands

```bash
cargo test -p ferrosa-udf
cargo test -p ferrosa-cql --lib route_create_function
```

Add router or integration tests for successful component invocation,
replacement, overload isolation, restart recompile, and replicated DDL apply.

## Related

- `ferrosa-cql/src/parser.rs::parse_create_function`
- `ferrosa-cql/src/router.rs::route_create_function`
- `ferrosa-cql/src/router.rs::evaluate_row_udfs`
- `ferrosa-udf/src/executor.rs`
- `ferrosa-udf/src/wit/ferrosa-udf.wit`
- `ferrosa-schema/src/metadata/function.rs`
- `examples/advanced-aggregation/`
- `examples/timeseries-rrd/custom-wasm-udf.cql`
