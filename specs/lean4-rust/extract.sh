#!/usr/bin/env bash
# Regenerate one extracted module of this package from the Rust it is about.
#
# The generated file is committed, because a chapter in the book cites it and a review reads the
# Lean the toolchain actually produced. Committed generated code is only honest if something can
# reproduce it: this script is that something, and `check-sync.sh` is what notices when it has not
# been run. Between them, "the extraction is part of the build" stops being a claim and becomes a
# file comparison.
#
# Usage: extract.sh <module>          (modules are listed in modules.txt)
# Env:   CHARON, AENEAS               pinned tools; see SYNC.md
set -uo pipefail
cd "$(dirname "$0")" || exit 2

# sha256 over the sorted (path, content) pairs of the sources an extraction is a function of.
source_hash() {
  ( cd "$1" && find $2 -type f -name '*.rs' -print0 2>/dev/null \
      | sort -z | xargs -0 sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1 )
}

pkg=$PWD
root=$(git -C "$pkg" rev-parse --show-toplevel) || exit 2
CHARON=${CHARON:-$HOME/src/aeneas/charon/bin/charon}
AENEAS=${AENEAS:-$HOME/src/aeneas/src/_build/default/main.exe}
# Charon must not share the workspace's cargo target dir: the rustc artifacts it needs are the ones
# it built, and mixing them with a normal `cargo build` produced `E0463 can't find crate` on a
# dependency that compiles fine on its own. The gap sweep isolates it the same way.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/.cache/charon-target}"
export PATH="$HOME/.local/aeneas:$PATH"
mkdir -p "$CARGO_TARGET_DIR"

module=${1:?usage: extract.sh <module>    (see modules.txt)}
row=$(awk -v m="$module" '$1==m && $0 !~ /^#/ {print; exit}' modules.txt)
[ -n "$row" ] || { echo "FAIL: '$module' is not in modules.txt"; exit 2; }
read -r _ crate sources scope <<<"$row"
crate_dir=$root/$crate
[ -d "$crate_dir" ] || { echo "FAIL: no crate at $crate_dir"; exit 2; }

# `*` matches one path segment, not a subtree: `m::*` picks the module's own items and leaves every
# `impl Type` block out, because a method's name is `m::Type::method` — two segments deep. That is
# why the first extraction of `compression` had the `Compression` enum and none of its methods, and
# why the claim about `decompress` had no symbol to be about. A scope is therefore a comma-separated
# list, and each pattern is passed to charon separately.
IFS=',' read -r -a scopes <<<"$scope"
scope_args=()
for s in "${scopes[@]}"; do scope_args+=(--start-from "$s"); done

# What this crate leaves out of the Lean, as charon name patterns. The list comes from
# `scope.toml`, not from this invocation: an exclusion that is not written down and justified in
# the scope file does not happen, and `scope-check.py` refuses an entry without a reason, an owner
# and a date. `--opaque` is used rather than `--exclude` on purpose -- an opaque item keeps its
# declaration (aeneas emits `def ... := sorry`, carrying the pattern in an `@[rust_fun "..."]`
# attribute), so the assumption is visible to a reader of the generated file. `--exclude` would
# delete it, and a deletion nobody can see is the failure mode this whole mechanism is for.
python3 "$pkg/scope-check.py" included "$crate" || {
  echo "FAIL: $crate is not in the extraction scope (specs/lean4-rust/scope.toml)"
  exit 1
}
opaque_args=()
while IFS= read -r pattern; do
  [ -n "$pattern" ] && opaque_args+=(--opaque "$pattern")
done < <(python3 "$pkg/scope-check.py" items "$crate")

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Charon compiles the crate and records spans; the scope keeps the module to its own chapter.
# `--dest-file` names the output, because a glob over the workspace root picks up whatever an earlier
# sweep left there: the first run of this script regenerated `Storage.lean` from a stale
# `ferrosa_alloc_count.llbc` and the diff was the only thing that said so.
llbc=$work/extract.llbc
( cd "$crate_dir" && "$CHARON" cargo --preset=aeneas --dest-file "$llbc" \
    "${scope_args[@]}" ${opaque_args[@]+"${opaque_args[@]}"} -- --lib ) > "$work/charon.log" 2>&1
[ -f "$llbc" ] || { echo "FAIL: charon produced no .llbc for $crate"; tail -5 "$work/charon.log"; exit 1; }

# charon drops a name pattern it cannot parse (`filter_map` over the parsed patterns in
# options.rs), so a malformed scope entry would silently exclude nothing. Say so here rather than
# let the run translate the item it was told to leave out.
if bad=$(grep -o "failed to parse pattern \`[^\`]*\`" "$work/charon.log"); then
  echo "FAIL: charon rejected a scope entry — $bad"
  echo "      an entry it cannot parse excludes nothing; fix specs/lean4-rust/scope.toml"
  exit 1
fi

"$AENEAS" -backend lean -dest "$work/out" "$llbc" > "$work/aeneas.log" 2>&1
generated=$(ls "$work"/out/*.lean 2>/dev/null | head -1)
[ -n "$generated" ] || { echo "FAIL: aeneas produced no .lean for $crate"; tail -5 "$work/aeneas.log"; exit 1; }

# Aeneas exits non-zero on an internal error in a *reachable* item while still writing the module, so
# its exit status alone does not say the output is sound. The internal errors are recorded because
# they are what makes the emitted Lean reference an external type it never declared.
internal=$(grep -c "Internal error" "$work/aeneas.log" 2>/dev/null || echo 0)

# A committed file that does not compile is worse than a stale one, so prove it here -- and put the
# previous file back when it fails. `tail -3` on a build log reports "build failed" and the failing
# target but never the error that caused it, so the failure is captured whole.
previous=$work/previous.lean
had_previous=0
[ -f "$pkg/FerrosaRust/$module.lean" ] && { cp "$pkg/FerrosaRust/$module.lean" "$previous"; had_previous=1; }
cp "$generated" "$pkg/FerrosaRust/$module.lean"

LEAN_NUM_THREADS=2 lake build > "$work/build.log" 2>&1 || {
  echo "FAIL: the regenerated FerrosaRust/$module.lean does not elaborate"
  grep -E "error:|Unknown identifier|unknown identifier" "$work/build.log" | head -10
  if [ $had_previous -eq 1 ]; then
    cp "$previous" "$pkg/FerrosaRust/$module.lean"
    echo "      previous FerrosaRust/$module.lean restored -- the recorded extraction still stands"
  else
    rm -f "$pkg/FerrosaRust/$module.lean"
  fi
  exit 1
}

# The scope is a claim about this file, and the file is the evidence: every scope entry has to be
# real. `record` asks charon's llbc whether the entry still names an item (charon says nothing for
# a pattern that matches nothing -- measured -- so a rotted entry would exclude nothing and stay
# silent) and the emitted Lean whether the assumption is visible there. Aeneas drops the
# declaration of an opaque trait-impl method that nothing reachable calls, so for those the row in
# scope.lock IS the record; for everything else the row must name a declaration that exists.
python3 "$pkg/scope-check.py" record "$module" --llbc "$llbc" --charon "$CHARON" || {
  echo "FAIL: an item entry in specs/lean4-rust/scope.toml matched nothing in the extraction"
  if [ $had_previous -eq 1 ]; then
    cp "$previous" "$pkg/FerrosaRust/$module.lean"
    echo "      previous FerrosaRust/$module.lean restored -- the recorded extraction still stands"
  else
    rm -f "$pkg/FerrosaRust/$module.lean"
  fi
  exit 1
}

python3 "$pkg/scope-check.py" check --module "$module" || {
  echo "FAIL: FerrosaRust/$module.lean and specs/lean4-rust/scope.toml disagree (see above)"
  if [ $had_previous -eq 1 ]; then
    cp "$previous" "$pkg/FerrosaRust/$module.lean"
    echo "      previous FerrosaRust/$module.lean restored -- the recorded extraction still stands"
  else
    rm -f "$pkg/FerrosaRust/$module.lean"
  fi
  exit 1
}
rm -f "$llbc"

# Record what the file was produced from, so a later run can tell whether it is still that. The
# toolchain belongs in the record: the same sources under a different translator are a different
# extraction, and the diff between the two files is the only place that shows.
hash=$(source_hash "$crate_dir" "$sources")
tools=$(printf 'aeneas %s charon %s' \
  "$(git -C "$HOME/src/aeneas" rev-parse --short HEAD 2>/dev/null || echo unknown)" \
  "$(awk '!/^#/ && NF {print $1; exit}' "$HOME/src/aeneas/charon-pin" 2>/dev/null || echo unknown)")
tmp=$work/sync.lock
if [ -f sync.lock ]; then grep -vE "^$module[[:space:]]" sync.lock > "$tmp"; else : > "$tmp"; fi
printf '%-10s %-16s %s  %s  %s  %s  ok\n' "$module" "$crate" "$hash" "$sources" "$scope" "$tools" >> "$tmp"
sort -o "$tmp" "$tmp"
mv "$tmp" sync.lock
[ "${internal:-0}" -gt 0 ] && echo "note: aeneas reported $internal internal error(s); the module elaborated anyway, but see SYNC.md"

# A committed file that does not compile is worse than a stale one, so prove it here.
LEAN_NUM_THREADS=2 lake build 2>&1 | tail -3
echo "regenerated FerrosaRust/$module.lean from $crate ($sources)"
