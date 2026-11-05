# Keeping the extraction and the model in sync

The corpus in `../lean4` proves claims about **models transcribed by hand** beside the code. A `.met`
claim's theorem is therefore true independently of what the Rust does: change the implementation and
the corpus stays green. This package closes that gap one item at a time — its subjects are items that
Charon and Aeneas produced **from the source**, so a proof here stops closing the moment the code
moves.

## What is committed and what is generated

    FerrosaRust/Storage.lean     generated from ferrosa-storage/src/count.rs   (committed)
    FerrosaRust/Sstable.lean     generated from ferrosa-sstable/src            (committed)
    FerrosaRust/Link/Count.lean  written by hand, about the generated module
    sync.lock                    what each generated file was produced from

Generated files are committed because a chapter cites them and a reviewer reads the Lean the
toolchain actually produced. That is only honest while something can reproduce them.

## The loop

    ./bootstrap.sh                 # link the Aeneas Lean backend (not vendored)
    ./check-sync.sh                # seconds; fails when the Rust moved under a generated file
    ./extract.sh Storage           # regenerate one module, rewrite its row in sync.lock, build
    lake build                     # including every Link theorem

`check-sync.sh` compares the hash of the sources an extraction is a function of — the `sources`
column of `modules.txt` — against the hash recorded in `sync.lock`. It needs no charon and no
aeneas, so it runs on every pull request. When it fires, the generated file is making claims about
code that no longer exists; regenerate, read the diff, and re-prove whatever the diff broke.

Where an aeneas checkout exists (`$AENEAS_DIR`, default `~/src/aeneas`), the check also compares
each row's recorded translator against it and fails on a mismatch. Where none exists, as on every CI
runner, it prints `TOOL    not compared` once and checks the sources alone. It never reports a
translator as having moved to a version it could not read.

A row's status is `ok` or `stale:<reason>`. `stale` means the extraction cannot be regenerated under
the installed toolchain, and the reason names what blocks it. The check reports it every run rather
than passing silently, because a generated file nobody can reproduce is a claim resting on nothing.

## What the scope buys, measured (and what generated code costs)

Extraction has a scope, and it is a file: `scope.toml`. Three kinds of entry, each with a reason, an
owner and a date — every ferrosa crate (30 of them), four dependencies named as *critical*, one
blanket exclusion for everything else, and `[[item]]` entries for anything left out inside an
included crate.

The decision behind it is that this package reasons about ferrosa's code, not about its dependency
tree. A blanket "everything else" is safe only because it is written down and machine-checked:
`scope-check.py` fails when a workspace crate has no entry (`UNSCOPED`), when an entry has no
reason/owner/date, when a second blanket exclusion appears, and when an item is left out that no
entry explains (`SILENT`). It reads the emitted Lean, not a flag somebody passed: charon's `--opaque`
makes aeneas emit the item as `def … := sorry` with its charon name pattern in an
`@[rust_fun "…"]` attribute, so "what was assumed" is greppable in a committed file.

Where the translator drops the declaration instead of carrying it (it does that for an opaque
trait-impl method nothing reachable calls), `scope.lock` carries the row. That ledger is written by
`extract.sh` through `scope-check.py record` from charon's llbc and the emitted Lean — never by hand
— and `check` refuses a row no scope entry explains, a row for a module with no generated file, two
rows for one entry, and a row that claims `in-lean` when the module has no such declaration. The
ledger exists because neither tool can be told "declare this and stop"; if it could, the entry would
be visible in the Lean and the row would say `in-lean`.

Measured 2026-11-04, `ferrosa-view` — the cheapest crate the ceiling blocks, 988 lines. Its
`#[derive(Deserialize)]` on `ViewMetadata` generates a visitor whose `visit_map` reaches **8,192
exits** (`widest=(w=8192, arms=[8191,1]) at metadata.rs 76:49-76:60`); the blocked run sat in that
one function for 13 minutes at 2.2 GiB and had not finished its first function at the 4 GiB ceiling.
With `--opaque` on the five derived `Deserialize` impls, under `lean-scoped-run.sh` (`MEM_MAX=4G`,
`-memory-limit-mb 3500`):

    llbc               23 MB → 8.6 MB (the visitor's 8,192-path body is not in it)
    aeneas             wall 27.9 s, peak 2,329 MiB, non-empty Lean emitted (437,356 bytes)
    internal errors    9 in the runs measured (they are the trait/lifetime failures below)

That is the ceiling answer: the crate translates. It still does not install, and the reason is
*not* the exclusion — aeneas reports `[Error] Unimplemented: found an occurrence of a lifetime
constraint` while translating the signature of `ViewMetadata::primary_key` and
`validate::base_pk_names`, both `-> impl Iterator<Item = &'_ str>`, and the emitted module then fails
to elaborate on the `core.iter.traits.iterator.Iterator` records (`chain`, `map`, `filter`, `collect`
are not fields of the structure). `--translate-all-methods` makes that worse (75 elaboration errors,
37 internal errors). So `modules.txt` has no `View` row and `scope.toml` has no `[[item]]` entry: an
entry no module can satisfy is the rot the checker exists to catch.

The cost of leaving generated code out is stated where it is paid: the wire format of `ViewMetadata`
is **assumed, not proved** — nothing in this package says a `ViewMetadata` serializes to the bytes
the schema revision promises. That is the assumption a reader should carry, and it is why the entry
carries a reason rather than a shrug.

Two facts about the tools are worth keeping, because both defeat a check someone would otherwise
trust:

  - charon accepts an `--opaque` pattern that matches nothing, exits 0 and says nothing (measured on
    `ferrosa-alloc-count`), so a rotted entry excludes nothing and looks fine. That is why `record`
    re-matches every entry against the llbc's own `// Full name:` lines.
  - aeneas emits no declaration at all for an opaque trait-impl method that nothing reachable calls,
    even with charon's `--start-from` marking it (measured: the item is `started_from=true`,
    `opacity=Opaque` in the llbc, and the Lean is byte-identical to the run without the exclusion).
    That is why the ledger exists rather than a rule "the Lean must show it".

## What is verified, and what is only asserted

  - `Sstable` (from `ferrosa-sstable/src/compression.rs`) regenerated at aeneas `a4c75c4`, twice,
    byte-identical. The copy committed before this work differed only as the translator changed:
    `axiom` became `def … := sorry`, plus the const closures the current scope reaches.
  - `Storage` (from `ferrosa-storage/src/count.rs`) is **stale**. Its committed content was
    produced by an earlier translator. Regenerating it under `a4c75c4` emits a call to
    `core.iter.Iterator.count`, which the Aeneas Std library has no model for, so the module does
    not elaborate and `extract.sh` restores the previous file. The three theorems in
    `Link/Count.lean` are about constants in `count.rs` that have not changed, so they still hold;
    what does not hold is any claim that this file is what today's toolchain produces.


## What "linked" does and does not mean yet

Three theorems in `Link/Count.lean` are stated over extracted constants, and two of the three
disagree with the first draft of the claim: `header_leaves_a_remainder` refuted "the header divides
into slots" in a build. That is the mechanism working.

It is **not** a refinement. No theorem here yet says "the corpus's claim about the commit log is
satisfied by the extracted write path". What exists is one module of the codebase pinned by
properties, and the honest count of corpus claims tied to extracted items is **zero**. The next step
is per-claim: take a ledger row whose subject is a specific function, extract it, and state the
refinement — the corpus says what must hold, this package says the code satisfies it.
