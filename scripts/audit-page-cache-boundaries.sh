#!/usr/bin/env bash
# Static inventory for filesystem I/O and cache-policy boundaries. Findings require workload review.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: audit-page-cache-boundaries.sh [--include-tests] [ferrosa-root]

Reports Rust source locations that cross filesystem or page-cache boundaries.
It does not claim that buffered I/O is wrong; classify the operation's reuse and
latency sensitivity before considering direct I/O or cache hints.
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

printf '# Ferrosa filesystem and page-cache boundaries: %s\n' "$root"
printf '# A memory map normally uses the page cache; direct I/O needs alignment and an explicit fallback.\n'

report 'Direct-I/O and cache-hint controls' \
  '(\bO_DIRECT\b|\bdirect[_-]?io\b|\bDirectIo\b|\bposix_fadvise\b|\bFADV_[A-Z_]+\b|\bDONTNEED\b|\bWILLNEED\b|\bSEQUENTIAL\b|\breadahead\b|\bsync_file_range\b)'
report 'Memory maps (not a page-cache bypass)' \
  '(memmap|Mmap(Mut|Options)?|mmap\()'
report 'Buffered filesystem reads and writes' \
  '(File::open|OpenOptions|std::fs::|tokio::fs::|AsyncReadExt|AsyncWriteExt|read_exact|write_all|read_vectored|write_vectored)'
report 'Durability and writeback boundaries' \
  '(fsync|fdatasync|sync_all|sync_data|flush\(|shutdown\()'
