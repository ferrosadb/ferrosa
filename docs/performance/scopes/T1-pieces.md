# T1 in pieces — each independently shippable

T1 as one task is a 61-call-site, three-type-change refactor. Split into six pieces, ordered so
that **value lands before risk**, and so that the compiler — not a grep — enumerates the blast
radius at the moment it exists.

Every piece: own branch, own gates, own commit. None of them depends on a later piece.

## Why this decomposition

Measured blast radius, src only:

| change | sites | risk |
|---|---|---|
| remove `vec![row.clone()]` / `key.clone()` on the hot path | **5** | none — no type change |
| `mutation_id` batching | **23** | correctness (replay dedup) |
| `keyspace`/`table` → `Arc<str>` | 708 / 1034 hits (upper bound; the pattern matches other types too) | high — `pub` fields, so every reader is a compile error |
| `rows` → `SmallVec`/`Arc<[Row]>` | 1460 hits (upper bound) | highest |

The two cheap pieces capture real allocation wins with **no type change at all**, and they land
first. The expensive pieces are type changes where the compiler is the census — so each gets its
own commit, and a stop rule.

---

## P0 — wire-format golden test (safety net, no production change)

`Mutation` is persisted to the commit log and replicated. Every later piece must leave the
encoding byte-identical, so this lands before anything moves.

- Encode a fixed mutation (two rows, mixed cell types, a legacy zero id) and assert a
  hard-coded sha256.
- Round-trip it: decode and assert field equality, including `mutation_id`.
- Include a mutation with the legacy `[0u8; 16]` id, because that is the sentinel that changes
  dedup behaviour.

**Gate:** `cargo test -p ferrosa-storage --lib commitlog::`
**Stop rule:** none — this piece cannot break anything.

## P1 — `mutation_id`: stop minting a UUID per write

23 sites; self-contained; correctness-sensitive because replay dedup depends on it.

Decision recorded in code: mint **one id per statement/batch** and share it across that batch's
mutations **only if** replay dedup still behaves — dedup is currently per-mutation, and a zero id
is always re-applied. If sharing an id would deduplicate distinct mutations, keep `new_v4()` per
mutation and record the RNG cost as accepted.

- Do not change the wire format (the id is still 16 bytes in place).
- Do not change replay behaviour silently. Test it either way.

**Tests:** replay a segment twice → each mutation applied once; a batch that shares one id is
deduplicated once (or explicitly is not, per the decision); legacy zero-id still re-applied.
**Gate:** `cargo test -p ferrosa-storage` + `-p ferrosa-cdc` (5 sites).

## P2 — the hot path stops cloning (no type change)

Five sites, ~1 hour of work, real allocation win:

- `ferrosa-storage/src/engine.rs:9720`, `:9837` — `vec![row.clone()]` → move the row
- `ferrosa-storage/src/engine.rs:9837` (`key.clone()`) — restructure so the key is moved
- `ferrosa-cluster/src/coordinator/write.rs:348`, `:652`, `:859` — `vec![row.clone()]` per replica

No struct fields change. This is the piece that proves the "one buffer in, one out" rule on the
real write path with zero blast radius.

**Test:** the caller's row is consumed (borrow-checker-enforced), and a 1k-row write uses the
same record-buffer capacity as a 10k-row write.
**Gate:** `cargo test -p ferrosa-storage -p ferrosa-cluster`, P0 still green.

## Why the type change is hard — and the two pieces that make it easy

The difficulty of P3b is not incidental, it is the smell. `Mutation` is a **persisted** type (so
the encoding is frozen), with **61 constructors**, and **`pub` fields** (so every reader is part
of its API). Three jobs in one type:

1. a wire DTO (frozen encoding),
2. a domain value (owned identity),
3. a public struct that 700+ sites read field-by-field.

The rule applies: **make the change easy, then make the easy change.** Do these first, and P3b
stops being a 700-site migration:

### PA — a `Name` newtype that derefs to `str`

```rust
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct Name(Arc<str>);
impl Deref for Name { type Target = str; }          // .as_bytes() / .len() / starts_with() still work
impl AsRef<str> for Name { .. }
impl fmt::Display for Name { .. }
impl PartialEq<str> for Name { .. }                 // m.keyspace == "bench" still compiles
impl From<&str> / From<String> for Name { .. }      // construction stays frictionless
```

Because it derefs to `str`, a field of type `Name` satisfies almost every read site unchanged —
method calls, comparison, `&str` coercion. Only sites that *require an owned `String`* break, and
those are the sites worth looking at. Its `Clone` is a refcount bump, which is the entire point.

Standalone and additive: no field changes, no call-site changes. Test that three clones share one
allocation (`Arc::as_ptr`), and that `Name` compares equal to a `&str`.

### PB — constructors accept `impl Into<Name>`

```rust
pub fn new(keyspace: impl Into<Name>, table: impl Into<Name>, ...) -> Self
```

One signature edit, and **all 61 call sites keep compiling** untouched: `"x".into()`, `String`,
`&str` and `Name` all satisfy `Into<Name>`. The hot path passes a `Name` it already shares; the
cold sites pass what they always passed.

After PA and PB, P3b is literally:

```rust
pub keyspace: Name,   // was String
pub table: Name,      // was String
```

and the compiler's remaining errors are only the places that genuinely need an owned `String` —
a handful, each a real decision rather than mechanical churn.

### The same trick for `rows`, if P4 ever happens

`Rows(Vec<Row>)` with `Deref<Target = [Row]>` keeps `.iter()`, `.len()`, indexing and slicing
working; `impl Into<Rows>` for `Row`, `Vec<Row>` and `SmallVec` keeps all 1460 read sites and
every constructor unchanged. That is what turns P4 from "the largest blast radius" into a
representation swap behind a newtype.

### What stays genuinely hard

The **frozen encoding** is not a smell to be refactored away — `Mutation` must decode what old
binaries wrote. That is why P0 exists and why it is not negotiable: the wire format is a contract
with data already on disk, and only additive encoders can change.

## P3a — introduce `TableRef` without changing `Mutation`'s fields

Additive only:

```rust
pub struct TableRef { pub keyspace: Arc<str>, pub table: Arc<str> }
impl Mutation { pub fn with_ref(...) -> Self }   // new constructor
```

`Mutation` keeps `keyspace: String` / `table: String`; `with_ref` stores both forms (or converts
internally). The write path at `:9837` starts using `with_ref` with a `TableRef` resolved once per
statement. **Zero existing call sites change.**

**Test:** two mutations from one statement share `Arc::as_ptr`. P0 still green.
**Gate:** `cargo test -p ferrosa-storage -p ferrosa-cluster`.

## P3b — flip the field types (the compiler is the census)

`keyspace: String` → `Arc<str>`, `table: String` → `Arc<str>`. Now build and fix what the
compiler names — do **not** grep first; the grep counts are contaminated by other types.

**Stop rule:** if the compiler reports more than ~40 distinct sites, stop, commit what compiles
behind a feature-free shim, and split into "readers that need `&str`" (mechanical `.as_ref()`)
and "sites that genuinely need an owned `String`" (decide per site).

**Gate:** storage + cluster + cql + postgres + graph; P0 still green.

## P4 — `rows` representation (last, and only if measured)

`Vec<Row>` → `SmallVec<[Row; 1]>` or `Arc<[Row]>`. 1460 upper-bound hits; the largest blast
radius of all. **Do this only if P2 + P3 leave a measured gap** — the clone removal in P2 may
already capture most of the win.

Warning recorded from the audit: `Mutation` sits in the observer queue and in parked-write sets,
so any inline capacity is multiplied by queue depth. Measure with the observer queue at capacity
before choosing an inline size above one row.

---

## Invariants that gate every piece

| id | invariant | where |
|---|---|---|
| I1 | commit-log encoding byte-identical | P0 test, re-run in every piece |
| I2 | replay dedup unchanged | P1 |
| I3 | data preserved: row bytes read back equal | P0 round-trip, re-run everywhere |
| I4 | identity shared not copied (`Arc::as_ptr`) | P3a |
| I5 | allocations flat in row count | P2, P4 |
| I6 | no test weakened to make a piece pass | all |

## Order and why

`P0 → P1 → P2 → PA → PB → P3a → P3b → (P4 if measured)`.

P0 first because it is the only thing that catches a wire-format change smuggled in with a type
change. P1 and P2 next because they are small and carry real value at low risk — they prove the
pattern before the expensive pieces. P3 is split because introducing `TableRef` additively is a
different risk class from flipping `pub` field types across 700+ call sites. P4 is last and
optional because it has the worst risk-to-reward of the set.
