---
crate: ferrosa-postgres
doc: roadmap
last_updated: 2026-09-27
---

# ferrosa-postgres — Roadmap

Sourced from in-code fail-loud `0A000`/preview gaps, the FMEA
([fmea.md](fmea.md)), and the dependency/usage review. There are no `TODO`/
`FIXME` markers in the source — the open work is encoded as fail-loud
`feature_not_supported` paths and documented lossy fallbacks instead.

## Done (recent)

- **PostgreSQL MVCC transaction semantics.** Explicit SERIALIZABLE begins
  pin a read timestamp; simple and extended SELECT use version overlays; buffered
  writes are visible to their own transaction; commit checks table-level read
  and write epochs in standalone mode and atomically applies through the local
  storage engine. In cluster mode a global PostgreSQL commit marker currently
  validates snapshots, conservatively aborting after any intervening PG commit.
  Write skew, predicate conflicts, lost updates, and real-time-order violations
  abort or return the committed value as appropriate; versions are reclaimed
  after the oldest active snapshot advances. Explicit isolation levels other
  than SERIALIZABLE fail loud. CQL transactions remain on Accord.
- **PostgreSQL strict-serializability workload.** The native-driver Jepsen test
  checks transfers, register updates, predicate/phantom histories, and
  write-skew histories. Its fault schedule pauses one replica and verifies the
  history and final state on the active quorum.
- **Parameterized DML** (was FMEA PG-2, `feat/pg-extended-crud`).
  `INSERT`/`UPDATE`/`DELETE` accept bound `$N` parameters over the extended
  protocol: prepared as `PreparedKind::{Insert,Update,Delete}`, substituted at
  `Execute` via `substitute_param` (fail-loud `08P01` on an unbound `$N`), with
  param OIDs inferred from each placeholder's target column. Transactional
  parameterized DML buffers into the same session write-set.
- **`INSERT … RETURNING col,…`/`*`** (part of FMEA PG-3, `feat/pg-extended-crud`).
  Echoes the just-written values as a `DataRow` (built in-memory, no storage
  read-back) — the `Ecto.Repo.insert` generated-key path; works inside a
  transaction (rows returned now, write commits at COMMIT).

## Now (highest value)

- **Replica catch-up and mixed-protocol correctness** (FMEA PG-11). The fault
  schedule checks PostgreSQL transaction histories while one replica is
  paused, then checks final-state convergence on the active quorum. Verify
  post-resume Accord catch-up and histories that mix PostgreSQL transactions
  with CQL writes before making those broader claims.
- **End-to-end SELECT streaming.** The storage provider is bounded, but
  `ferrosa_sql` collects base scans/results and PostgreSQL rendering buffers all
  wire messages. Stream through the executor and socket writer to bound memory.

## Next

- **`UPDATE`/`DELETE … RETURNING`** (FMEA PG-3) — today fail loud `0A000`; only
  `INSERT … RETURNING` is wired.
- **`ON CONFLICT` (upsert)** — today a parse error; the common ORM upsert idiom.
- **`= ANY($N)` / IN-list parameter expansion** — Ecto `where: x in ^ids`.
- **Multi-row `INSERT ... VALUES`** and richer `UPDATE`/`DELETE` `WHERE`
  (range/non-key predicates), which today are restricted to single-row, full-PK
  equality.
- **Binary `numeric`** result/param encoding (FMEA PG-7), removing the
  text-bytes fallback.
- **Real query cancellation** (FMEA PG-8) — mint a real `BackendKeyData`
  cancel key. (TLS on the wire, statement authorization and the shared
  failed-login limiter landed in t_e1c819ad.)
- **Mutual TLS** — client-certificate authentication for the PG listener
  (tracked with the internode mTLS work, t_b6c820f4).
- **Harden the SCRAM unknown-role oracle** — run the exchange against a dummy
  verifier so `UnknownRole` is not a user-enumeration signal (threat-model note
  in `handshake.rs`).

- **DDL follow-ups to T-132a** — `DROP`/`ALTER TABLE` (T-132b); DDL through the
  extended protocol (`Parse` refuses it); `NoticeResponse` so `IF NOT EXISTS`
  can emit the PG NOTICE; `timestamptz` (no `pg_types` entry, refused `42704`);
  enforcing `varchar(n)` / `numeric(p,s)`; cluster-mode jsonb DDL gate
  (T-300).

## Later

- **CQL `Duration` + collections** (`List`/`Set`/`Map`/`Tuple`/`Udt`/`Vector`)
  support (FMEA PG-4) — widen `ferrosa_sql::Value` and the
  `cql_to_value`/`value_to_cql` bridges. Until then, scans fail explicitly when
  they encounter one of these values.
- **jsonb slot in `pg_types`** (T-161a): add the `CqlType` arm and `ALL_PG_TYPES`
  entry for OID 3802 once type threading lands; constants are already reserved.
- **Exact float/numeric text-format parity** with Postgres (FMEA PG-9).
- **Real affected-row counts** for `UPDATE`/`DELETE` (FMEA PG-10) — read-before-
  write so the count reflects matches rather than always reporting `1`.
- **Session GUCs** (`SET`/`RESET`) and a broader scalar-function surface
  (`now()`, etc.).
- **More `pg_catalog`/`information_schema` coverage** as drivers/ORMs demand it.

## Non-goals

- Query planning / binding / relational operators — those live in `ferrosa-sql`.
- The storage row encoding — that is `ferrosa-row-bridge` (shared with CQL, D10).
- Cassandra wire compatibility — that is the CQL front-end (`ferrosa-cql`).

## jsonb (T-150)

Done: type threading (T-150); engine `Value::Jsonb` and the storage mapping (T-160). Remaining: T-161a: OID 3802 text/binary codec, jsonb input parsing, DDL name `jsonb` in `cql_type_for_pg_name`.
