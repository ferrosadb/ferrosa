# Eliminating the Lean elaboration reds

Status: in progress. Measured 2026-10-31 with aeneas `9b1eec9` (branch
`fix/unsupported-sig-is-not-fatal`, merged as ferrosadb/aeneas#1 at
`90110883`), charon `7b462d9159db2ed3015162352183906a30bfafc6`.

Source of the numbers: `ferrosa/specs/lean/gaps/*.json`, produced by
`scripts/lean_gap_inventory.py` (charon -> aeneas -> `lake env lean`). 1057 reds.

## What the reds actually are

Grouped by mechanism, not by Lean error kind:

| count | share | mechanism | owner |
|---|---|---|---|
| 414 | 39% | foreign trait declared inside a `mutual` block whose members have different parameter lists | aeneas printer (design) |
| 251 | 24% | reference to `<Type>.Insts.<Trait>…` with no matching declaration (242 crate-local, 195 of them `ferrosa-net`) | aeneas naming |
| 242 | 23% | `synthInstanceFailed`, mostly echoes of the two above | downstream |
| 89 | 8% | `noncomputable` missing on definitions that use `*.ctorElim` (all `ferrosa-net`) | aeneas header |
| 40 | 4% | crate-local structure field missing | aeneas / model |
| 13 | 1% | `Aeneas.Std` field missing (`write_u8`, `write_usize`, `Limits`, `record_fault`) | Lean std library |
| 8 | 1% | crate-local name missing | aeneas |

The serde case is the clearest instance of the first row. Lean rejects the group
outright:

```
error: Invalid mutually inductive types: `B` has 3 parameter(s), but the previous ...
Note: All inductive types declared in the same `mutual` block must have the same parameters
```

and a serde group has members with 1, 10 and 3 parameters, so the block never
elaborates and every later reference to any member reports `Unknown identifier`.
One fault, hundreds of reds.

## Step 1 — `noncomputable` (89 reds) — DONE (PR #2)

Root cause: the machinery existed and was unreachable. `Translate.ml` emitted

```ocaml
if fi.noncomputable then Printf.fprintf out "... noncomputable section\n";
```

but `fi.noncomputable` was hardcoded `false` at four construction sites and
`has_opaque && not all_computable` at two others — and `Config.all_computable`,
the flag whose help text promises exactly this section, was only half read.

Fix: the section is controlled by `-all-computable` alone, everywhere.

Two bugs surfaced on the way, both worth keeping in mind for the rest:

- The section was **never closed**. The footer only emitted `end <namespace>`,
  so any file that received the section was left with an unclosed section. It
  had never been noticed because the section was only emitted under
  `has_opaque`, which no test crate satisfied. The footer now emits `end`.
- Regenerating `tests/lean` from a binary that has a change is not enough: the
  source change must be in the same commit, or CI regenerates the fixtures with
  the old generator and diffs them against your new ones. That is exactly how
  the first attempt failed.

Acceptance: the generated header contains `noncomputable section` unless
`-all-computable`, and a crate that previously failed to elaborate for want of the
section elaborates.

Measured, and this is the correction worth reading before trusting the counts:
`ferrosa-net`'s `tls` module does **not** drop its 89 `lean.dependsOnNoncomputable`
reds. All 89 point at one line — `inductive rustls.enums.CipherSuite` — and they
survive with and without the section (89 errors either way on the same `llbc`). The
underlying message is

```
tag for constructor 'ferrosa_net.rustls.enums.CipherSuite.Unknown' is too big,
this is a limitation of the current compilation scheme
```

so the enum cannot be compiled at all, and every reference then reports a
`noncomputable` dependency on the generated `CipherSuite.ctorElim` that Lean would
have built. The inventory labels these `lean.dependsOnNoncomputable` because of the
wording, which is why they looked like this step's business. They are not: Rust
discriminants in the thousands (`0x1301` and up) exceed Lean's constructor tag limit.
That is its own fault, and it needs `discriminant` values or a different model for
wide enums — not a section.

## Step 2 — instance naming (251 reds, plus echoes)

Two conventions for the same instance name: the reference uses Charon's raw
item name (`ferrosa_net.bytes.bytes.Bytes.Insts.BytesBufBuf_implBuf`), the
declaration uses `ExtractBase.ctx_compute_trait_impl_name_raw`
(`<self type><Trait><Args>`). One name function, used by both sides, is the fix;
then re-measure. `ferrosa-net` carries 195 of the 251 and is the cheapest
target.

Acceptance: for every generated file, every referenced `.Insts.` name has a
matching declaration. Checkable by parsing the emitted file, no Lean needed.

Measured on `ferrosa_net`'s `tls` module: 39 distinct `.Insts.` references, 8 with no
declaration in the file, and only 3 of those are real gaps —
`rustls_pki_types.{PrivateKeyDerStatic,CertificateDerStatic}.Insts.Rustls_pki_typesPemPemObject{,Filter}`,
`rustls_pki_types.pem.ReadIter.Insts.CoreIterTraitsIteratorIteratorResultTError`,
`tokio_rustls.server.TlsAcceptor.Insts.CoreConvertFromArcServerConfig`,
`tracing_core.metadata.Level.Insts.CoreCmpPartialOrdLevelFilter`. The rest
(`core.iter.adapters.map.Map.Insts…`, `core.result.Result.Insts.CoreOpsTry…`) are
declared in the Aeneas library under `Aeneas.Std` and resolve there.

The gaps share one property: they are impls on *foreign* types, and they are absent from
`crate.trait_impls` entirely — charon never emitted them, because the extraction was
narrowed to this crate. So no iteration over the crate can stub them, and a condition
change inside the existing stub pass (stub when the *impl* is untranslated rather than
when the *trait* is) is a no-op here: it produced a byte-identical file, and was
reverted for want of a test that fires. The stub has to be collected at the *reference*
site, where the printed name and its trait application are already known, and flushed
after the declaration loop.

## Step 2b — wide enums: the 245-constructor ceiling (dominant red in `ferrosa_net`)

Measured on Lean 4.35, by generating inductives directly (one payload constructor so
the scalar and boxed representations are both covered):

```
244 constructors  ok
245 constructors  error: tag for constructor 'Wide.c244' is too big,
                  this is a limitation of the current runtime
```

The runtime reserves the top of the scalar tag space, so an inductive carries at most
244 constructors. `rustls::enums::CipherSuite` has **377**, which is why the whole
enum fails to compile and every later mention reports a phantom dependency on the
`CipherSuite.ctorElim` Lean would have generated — 94 of the 101 elaboration errors in
`ferrosa_net`'s `tls` module are that one cascade.

No printer change can fix this: the construct cannot be represented as an inductive at
all. The options are (a) a non-inductive model — a tag field over `Std.U16` plus
constructor `def`s, which changes what pattern matching compiles to; (b) `opaque` with
axioms for its operations, losing constructors; (c) `--opaque`/`--exclude` for such enums
at charon, which is user-side and leaves the type unmodelled. Choosing is a design
decision, not a bug fix, and (a) has the widest blast radius because `Discriminant` and
every `match` over the enum are generated from the inductive shape.

## Step 3 — the trait model (414 reds)

Decision: a trait is a **record of methods parameterised by its associated
types**, not an inductive family. `mutual` stays for RecGroups of data types and
is removed for trait groups.

Recursion between traits is interface recursion — `Serialize::serialize` takes
a `Serializer`, whose parent clauses name `SerializeSeq`, whose
`serialize_element` is generic over `Serialize`. Lean requires uniform
parameters for a mutual block precisely because it models one indexed family, so
the construct cannot accept a family whose parameters are by definition
non-uniform. Two candidate resolutions:

1. **Parameterise the sibling reference** (correct model): turn the forward
   reference inside the cycle into a parameter supplied at each use site, so the
   declarations become acyclic. Cost: every instantiation and impl gains an
   argument.
2. **Opaque cut** (triage): declare the cut member `def … := sorry`, keep the
   rest real, count the hole. Cost: every impl of the cut trait becomes `…
   := sorry` — the holes scale with impl count, not method count.

Do 1 where it is affordable, 2 elsewhere, and record which was used per group in
the generated file.

Acceptance: no `mutual` block contains members with differing parameter counts
(parseable check), and the serde crate drops its 343 reds.

## Step 4 — field and instance models (53 reds)

Crate-local fields (40) are emission bugs; `Aeneas.Std` fields (13) are library
gaps of the same shape as the deferred `t_6b9baa8a` (`core::str::pattern`).

## Blocking hole: the other backends

`Translate.ml` emits boundary stubs as Lean text (`def X … : Type := sorry`)
for **every** backend. A Coq run of `jsonb.llbc` produced a 20724-line file
containing 31 Lean-syntax lines — Coq cannot parse them.

The obvious repair, making the Coq registration of `Prop`-inductive eliminators a
fatal error so the offending definitions are skipped, is worse than the disease: the
same input then produced an 11-line file, an empty module, because the abort takes the
rest of the crate with it. Two independent variants — aborting at the registration
site, and recording the eliminator before aborting — both produced the same 11 lines.
The Lean-syntax leak has to be fixed at emission, not by refusing the input.

Gating the printing is not a small change: the same block is what registers the
trait and instance names in the extraction context, and the rest of the
extraction resolves them through it. Measured, deterministically:

```
unmodified binary   : 20724 lines      (twice)
trait block gated   :    41 lines
impl block gated    :    11 lines
both gated          :    11 lines
```

So the fix is to separate *registration* from *printing* — register
unconditionally, print per backend — not to guard the block. Until that is
done, any trait work risks silently emptying the Coq, F* and HOL4 output while
`dune build` stays green.

## Verification recipe

```bash
# one crate
python3 scripts/lean_gap_inventory.py --only ferrosa-net        # ferrosa repo
# one llbc, no toolchain
~/src/aeneas/src/_build/default/main.exe -backend lean -dest /tmp/x ./foo.llbc
# cross-backend regression (must not be empty, must not contain `def `)
~/src/aeneas/src/_build/default/main.exe -backend coq  -dest /tmp/y ./foo.llbc
```

CI: `ferrosadb/aeneas` now runs workflows (run 41248564230). `lean`,
`diff_lean_files`, `charon-pin-*`, `userdocs`, `select-runner` pass; `nix` fails
on `aeneas-check-tidiness` from formatting drift in files untouched by this
work (`dune build @fmt` reports `Config.ml`, `interp/InterpExpressions.mli`,
`llbc/TypesAnalysis.ml` and six more). Pinned formatter is 0.27.0.
