# Title

extract: do not emit a trait impl whose trait declaration was not extracted

# Body

A narrowed extraction (`--start-from`) keeps a `rust_trait_impl` but can drop the trait declaration
it names, because that declaration lives in `core`, not in the crate being extracted. The emitted
Lean then refers to a type that was never declared.

## Reproduction

`repro-dyn-error/` in this branch, 25 lines, nothing exotic — a crate that converts a
`Box<dyn Error>` the way most crates do:

```rust
impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self { Error::InvalidData(e) }
}

pub fn check(n: usize) -> Result<usize, Error> {
    if n == 0 { return Err("n must be non-zero".into()); }
    Ok(n)
}
```

```
charon cargo --preset=aeneas --start-from 'repro_dyn_error::*' -- --lib
aeneas -backend lean repro_dyn_error.llbc
lake env lean ReproDynError.lean
```

## Actual

Aeneas reports errors only inside `std` and still writes the file. The file contains

```lean
@[reducible, rust_trait_impl "core::convert::From<Box<@>, &'0 str>"]
def Box.Insts.CoreConvertFromShared0Str : core.convert.From (Dyn (fun _dyn =>
  core.error.Error _dyn)) Str := {
  sorry
}
```

and never declares `core.convert.From`. Compiling it fails with

```
error: failed to synthesize instance of type class
  Singleton ?m.2 (core.convert.From (Dyn fun _dyn => core.error.Error _dyn) Str)
```

The `Singleton` error is an artefact: with the type unelaborable, the body `{ sorry }` is read as
singleton notation. Nothing in the message names the missing declaration.

## Expected

Any one of:

1. include a referenced trait declaration transitively whenever an impl of it is extracted, so
   narrowing cannot separate them;
2. fail the extraction, naming the trait, instead of writing a file that cannot elaborate;
3. in addition, print `⟨sorry⟩` rather than `{ sorry }` for a `rust_trait_impl` body, so an
   unelaborable type is reported at that line.

## Scope

Triggered whenever extraction is narrowed, which is the only practical way to translate a crate whose
full call graph reaches a large dependency set — a full-crate run on the crate that produced this was
OOM-killed. Reported from `ferrosa-sstable` (587 lines extracted, 2 of them unelaborable, both this).

Two neighbouring limits found in the same run, likely already known: `thread_local!` statics (the
translator enters `std::sys::thread_local`) and `Iterator` trait defaults (`count`, `fold`), which
disappear from a narrowed extraction in the same way `core::convert::From` does.

## Environment

charon `nightly-2026.10.30`, aeneas `nightly-2026.10.30-de16f3c`, Lean `v4.35.0`.
