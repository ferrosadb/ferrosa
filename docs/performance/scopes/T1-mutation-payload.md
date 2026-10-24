# T1 scope — `Mutation` payload without per-write allocation

**Status:** scoped, not started
**Target:** `ferrosa-storage/src/commitlog/mutation.rs` + the write path at `engine.rs:9837` / `:9720`
**Programme position:** first item of Phase 2 (after class F, which is landed)

## 1. What the code actually does today

```rust
// ferrosa-storage/src/commitlog/mutation.rs:61
#[derive(Debug, Clone)]
pub struct Mutation {
    pub mutation_id: [u8; 16],   // Uuid::new_v4() minted inside Mutation::new
    pub keyspace: String,
    pub table: String,
    pub key: DecoratedKey,       // key bytes + cached token
    pub rows: Vec<Row>,
    pub timestamp: i64,
}

pub fn new(keyspace: String, table: String, key: DecoratedKey, rows: Vec<Row>, timestamp: i64) -> Self {
    Self { mutation_id: Uuid::new_v4().into_bytes(), keyspace, table, key, rows, timestamp }
}
```

And at `engine.rs:9837` (unconditional, every write) and `:9720` (when observers exist):

```rust
let mutation = Mutation::new(
    table_id.keyspace.clone(),   // String alloc + copy
    table_id.table.clone(),      // String alloc + copy
    key.clone(),                 // key bytes + token copy
    vec![row.clone()],           // Vec alloc + full row clone
    timestamp,
);
```

Per write, before any work is done: **up to 5 allocations, 2 full payload copies, and one
`Uuid::new_v4()`** — the last of which was not in the original audit and costs an RNG call per
write. `T2` (`mutation.clone()` at `:9338`) then deep-clones this whole struct once per
registered observer, so fixing T1 shrinks T2 as a side effect.

## Delivery plan

Split into six independently shippable pieces, ordered so value lands before risk:
[`T1-pieces.md`](T1-pieces.md). P0 wire-format net, P1 mutation_id, P2 hot-path clones,
P3a additive TableRef, P3b field flip, P4 rows representation (optional, measured).

## 2. Blast radius, and the decision that keeps it small

`Mutation::new(` has **61 src call sites**: cluster 21, storage 9, cql 8, postgres 7, graph 6,
ferrosa 2, ctl 1 (plus test files). Changing the signature means touching all of them.

**Decision: do not change the signature.** Add a second constructor and keep `new` as an
interning wrapper:

```rust
pub fn new(keyspace: String, table: String, key: DecoratedKey, rows: Vec<Row>, ts: i64) -> Self
    // unchanged for all 61 call sites; converts into Arc<str> internally

pub fn with_ref(table: TableRef, key: DecoratedKey, rows: impl Into<RowSet>, ts: i64) -> Self
    // the hot path uses this; no name allocation at all
```

This turns a 61-site refactor into a 2-site one plus a struct change, and it keeps the 60 cold
sites (control plane, tests, tooling) untouched — which is also what keeps the diff reviewable.

## 3. Work items

### T1a — shared identity in `Mutation`

```rust
pub struct TableRef { pub keyspace: Arc<str>, pub table: Arc<str> }
// Mutation: pub table_ref: TableRef  (keep `keyspace()`/`table()` accessors returning &str)
```

Per statement, resolve `TableRef` once; every mutation from that statement shares it, so the
clone in T2 becomes two refcount bumps.

**Test (cannot pass for a copy):** two mutations built from one statement have
`Arc::as_ptr(m.keyspace()) == Arc::as_ptr(n.keyspace())`.

### T1b — rows without a one-element `Vec` clone

`rows: Vec<Row>` → `SmallVec<[Row; 1]>` (or an `Arc<[Row]>` where observers need to share), and
the hot path **moves** the row instead of `vec![row.clone()]`.

**Test:** a single-row write performs one row move — assert by writing 10k rows and 1k rows and
comparing record-buffer capacity, per the audit's flat-in-row-count metric. Also assert the row
is not duplicated: the caller's row is consumed, which the borrow checker enforces if the
signature takes it by value.

### T1c — key by move

`key.clone()` at `:9837` exists only because the caller keeps `key` afterwards. Either restructure
so the key is moved (preferred), or make `DecoratedKey` shareable (`Arc<[u8]>` + token) so the
clone is a refcount bump.

**Test:** the token cached in `DecoratedKey` is not recomputed on the write path (assert the
cached-token constructor is called once per statement, not per mutation).

### T1d — `mutation_id` per write (separate, correctness-sensitive)

`Uuid::new_v4()` per mutation is an RNG call on the hot path. It exists for commit-log replay
deduplication, so this is **not** a free change:

- mint one id per statement/batch and share it **only if** replay dedup semantics still hold
  (dedup is per-mutation today: "a zero id is always re-applied");
- otherwise keep v4 and record the cost as accepted.

**Test:** replaying a commit-log segment twice applies each mutation once (the existing dedup
behaviour), and a batch sharing one id is still deduplicated correctly — or is explicitly not,
if the decision went the other way. Do not change dedup behaviour silently.

## 4. Invariants

| id | invariant | test |
|---|---|---|
| I1 | **Commit-log wire format is byte-identical** — `Mutation` is persisted, so `Arc<str>`/`SmallVec` must not change the encoding | encode a fixed mutation before and after; compare bytes (and sha256) |
| I2 | Identity is shared, not copied | `Arc::as_ptr` equality across mutations of one statement |
| I3 | Data preserved | row bytes read back equal what was written, for 1 / 100 / 10k rows |
| I4 | Dedup semantics unchanged | replay a segment twice; each mutation applied once |
| I5 | Allocations flat in row count | record-buffer capacity equal for a 1k-row and a 10k-row write |
| I6 | Behaviour unchanged where it is observable | existing `ferrosa-storage` suite green, no test weakened |

## 5. Gates

```
cargo test -p ferrosa-storage
cargo test -p ferrosa-cluster      # Mutation crosses the wire to replicas
cargo clippy -p ferrosa-storage -p ferrosa-cluster --all-targets -- -D warnings
cargo fmt -p ferrosa-storage -p ferrosa-cluster
```

`ferrosa-cluster` is in the gate set because `Mutation` is serialized for fan-out; a type change
that alters sizing or framing shows up there. Do not run the suites concurrently — concurrent
builds on this machine have produced spurious heal-timeout failures.

## 6. Out of scope

- `engine.rs:9338` (T2) — becomes cheap once T1a lands; do not also refactor it here.
- The graph key-derivation branch (`refactor/key-derivation`) and the attribute/index design
  notes — separate work, different crate area.
- The `_ => ` sites at `:9720`/`:9837` beyond what T1 needs; no unrelated rewrites.
- Changing the channel type, the observer path, or `ParkedWriteSet` ownership.

## 7. Risks

- **`Mutation` is persisted and replicated.** The single biggest risk is a wire-format change
  smuggled in with a type change; I1 exists for that reason and must be written first.
- **A one-element `SmallVec` inline capacity** trades memory per `Mutation` for an allocation.
  `Mutation` is queued in the observer channel and in parked-write sets, so the inline size is
  multiplied by queue depth — measure with the observer queue at capacity before committing to
  an inline size above one row.
- **`DecoratedKey` carries a cached token**; moving instead of cloning must not drop the token or
  recompute it, which would trade an allocation for a hash.
