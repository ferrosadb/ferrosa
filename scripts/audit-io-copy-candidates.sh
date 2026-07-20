#!/usr/bin/env bash
# Static inventory for I/O-path copies and materialization. Findings require call-path review.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: audit-io-copy-candidates.sh [--include-tests] [ferrosa-root]

Reports Rust source locations that can copy bytes or materialize a stream.
The output is a review queue, not a correctness or performance verdict.
USAGE
}

include_tests=false
root="."

while (($#)); do
  case "$1" in
    --include-tests) include_tests=true ;;
    --help|-h) usage; exit 0 ;;
    -*) printf 'Unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    *) root="$1" ;;
  esac
  shift
done

if ! command -v rg >/dev/null 2>&1; then
  printf 'rg is required to scan Rust sources.\n' >&2
  exit 127
fi
if [[ ! -d "$root" ]]; then
  printf 'Ferrosa root does not exist: %s\n' "$root" >&2
  exit 2
fi

globs=(--glob '*.rs' --glob '!target/**' --glob '!**/target/**' --glob '!**/.git/**')
if [[ "$include_tests" != true ]]; then
  globs+=(--glob '!**/tests/**' --glob '!**/benches/**' --glob '!**/examples/**')
fi

report() {
  local title="$1"
  local pattern="$2"
  printf '\n## %s\n' "$title"
  rg -n --no-heading "${globs[@]}" -e "$pattern" "$root" || true
}

printf '# Ferrosa copy and materialization candidates: %s\n' "$root"
printf '# Review each hit in context. Excludes tests by default; add --include-tests to include them.\n'

report 'Whole-file and full-body reads' \
  '(read_to_end|read_to_string|std::fs::read\(|tokio::fs::read\(|fs::read\(|\.bytes\(\)\?|\.collect\(\)\?\.to_bytes\(\))'
report 'Explicit byte copies' \
  '(copy_from_slice|clone_from_slice|extend_from_slice|Bytes::copy_from_slice|\.to_vec\(\)|Vec::from\([[:space:]]*&)'
report 'Potential stream materialization' \
  '(collect::<[[:space:]]*Vec|collect::<Vec|\.collect\(\)[[:space:]]*;|join_all\(|try_join_all\()'
report 'Serialization or protocol ownership boundaries' \
  '(serde_(json|cbor|bincode)::(to_vec|to_writer)|encode\([^;]*\.to_vec|BytesMut|Vec::<u8>::with_capacity)'
