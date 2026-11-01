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
