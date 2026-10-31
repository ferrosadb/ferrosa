# `impl From<Box<dyn Error>>` extracted with `--start-from` produces a file that cannot elaborate

## What happens

```
charon cargo --preset=aeneas --start-from 'repro_dyn_error::*' -- --lib
aeneas -backend lean repro_dyn_error.llbc
lake env lean ReproDynError.lean
```

Aeneas reports errors only inside `std` (`alloc::io::error::Error::other`,
`alloc::boxed::convert::From<&str> for Box<dyn Error>`) and still writes the file. Compiling it then
fails with:

```
error: failed to synthesize instance of type class
  Singleton ?m.2 (core.convert.From (Dyn fun _dyn => core.error.Error _dyn) Str)
```

## Why that message is misleading

The generated file contains

```lean
@[reducible, rust_trait_impl "core::convert::From<Box<@>, &'0 str>"]
def Box.Insts.CoreConvertFromShared0Str : core.convert.From (Dyn (fun _dyn =>
  core.error.Error _dyn)) Str := {
  sorry
}
```

and **never declares `core.convert.From`** — no `@[rust_trait] structure`, no axiom. Because the
type of the definition does not elaborate as a structure, the body `{ sorry }` is read as
`Singleton.singleton sorry`, and instance synthesis is reported against `Singleton`. The actual fault
is a trait impl emitted without the trait declaration it refers to, and the diagnostic names neither.

`core.convert.FromSame` is referenced the same way elsewhere in the file, also undeclared.

## Where the declaration went

`core.convert.From` and `core.convert.FromSame` do not exist anywhere in the Aeneas Lean standard
library:

```
grep -rn "core.convert.From" ~/.local/aeneas/backends/lean/Aeneas/Std/*.lean   -> no matches
grep -rn "FromSame"          ~/.local/aeneas/backends/lean/Aeneas/Std/*.lean   -> no matches
```

So they are expected to be *generated per crate*, and the narrowing dropped the declaration while
keeping the impl that names it. The impl is then emitted against a type that never existed, and Lean
reads the structure literal as singleton notation.

The fix I would suggest, in order of preference:

1. Transitively include a referenced trait declaration whenever an impl for it is extracted, so
   narrowing cannot separate the two.
2. Otherwise fail the extraction, naming the trait, rather than writing a file that cannot elaborate.
3. Additionally: emit `⟨sorry⟩` rather than `{ sorry }` for a `rust_trait_impl` body, so an
   unelaborable type produces a type error at that line instead of a `Singleton` synthesis failure.

## What is expected

Either

1. emit the referenced trait declaration alongside the impl, or
2. refuse the extraction with a message naming the trait that was not extracted, or
3. at minimum, make the emitted definition elaborable — the body of a `rust_trait_impl` is a
   structure literal, and printing `{ sorry }` invites singleton notation wherever the type is
   ambiguous.

## General or specific?

General. The trigger is `--start-from` narrowing — which is the only way to extract a large crate,
since a full-crate run on a tree with tokio/objects-store dependencies is OOM-killed. Any crate that
narrows its extraction and has a `Box<dyn Error>` conversion hits it.

Two neighbouring limits, general but likely known: `thread_local!` statics (the translator enters
`std::sys::thread_local` and fails), and `Iterator` trait defaults (`count`, `fold`), which vanish
from a narrowed extraction the same way `core::convert::From` does.

## Observed on

charon `nightly-2026.10.30`, aeneas `nightly-2026.10.30-de16f3c`, Lean `v4.35.0`.
