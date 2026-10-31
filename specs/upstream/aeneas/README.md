# Upstream: Aeneas cannot extract a `Box<dyn Error>` conversion

What follows is a defect we hit extracting this repository into Lean with Charon and Aeneas, kept
here because it blocks the specification work rather than the product: red claims in `specs/` are
tied to Rust source lines by the extraction, and green claims cannot be tied to anything until the
extraction type-checks.

- `ISSUE.md` is the write-up as it would be filed upstream.
- `repro-dyn-error/` is a 25-line crate that reproduces it, nothing exotic: a `From<Box<dyn Error>>`
  impl of the kind every crate that reports parse failures has.

Status: fork at `github.com/ferrosadb/aeneas`, branch `fix/trait-impl-without-decl`, carrying the
repro only. The fix belongs in `src/extract/` and is not written yet — it needs the OCaml toolchain
to compile and iterate, which is being installed.

Two neighbouring limits found in the same run, likely known upstream and not filed here:
`thread_local!` statics (the translator enters `std::sys::thread_local` and fails) and `Iterator`
trait defaults (`count`, `fold`), both of which vanish from a narrowed extraction the same way
`core::convert::From` does.
