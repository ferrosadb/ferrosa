#!/usr/bin/env bash
# Gap-report *sample* per crate: one representative module instead of the whole crate.
#
# Whole-crate extraction does not finish for the large crates (charon runs for many minutes on
# ferrosa-common/ferrosa-cql/ferrosa-ctl), so a crate-level sweep reports `timeout` for them and
# says nothing about the code. Picking the biggest module gives a measurable sample per crate.
#
# Usage: lean_gap_sample.sh <crate> [<crate> ...]
set -uo pipefail
REPO=${REPO:-$HOME/src/ferrosa-suite/ferrosa}
OUT=${OUT:-/tmp/gapsweep-sample.tsv}
PER_MODULE_TIMEOUT=${PER_MODULE_TIMEOUT:-110}

cd "$REPO" || exit 2
[ -s "$OUT" ] || printf 'crate\tmodule\tstage\ttranslation_errors\tlean_errors\tsorry\tseconds\n' > "$OUT"

for c in "$@"; do
  if cut -f1 "$OUT" | grep -qx "$c"; then echo "skip $c"; continue; fi
  dir=$(python3 - "$c" <<'PY'
import json, subprocess, sys
crate = sys.argv[1]
meta = json.loads(subprocess.run(['cargo','metadata','--no-deps','--format-version','1'],
                                 capture_output=True, text=True).stdout)
for p in meta['packages']:
    if p['name'] == crate:
        print(p['manifest_path'].rsplit('/', 1)[0]); break
PY
)
  [ -n "$dir" ] || { printf '%s\t-\tlocate\t-\t-\t-\t0\n' "$c" >> "$OUT"; continue; }

  # Biggest module file under src/, skipping the crate root.
  mod=$(python3 - "$dir" <<'PY'
import sys, pathlib
root = pathlib.Path(sys.argv[1]) / 'src'
best, best_size = None, -1
for f in root.rglob('*.rs'):
    if f.name in ('lib.rs', 'main.rs'):
        continue
    rel = f.relative_to(root).with_suffix('')
    parts = [p for p in rel.parts if p != 'mod']
    if not parts:
        continue
    if f.stat().st_size > best_size:
        best, best_size = '::'.join(parts), f.stat().st_size
print(best or '')
PY
)
  [ -n "$mod" ] || { printf '%s\t-\tno_module\t-\t-\t-\t0\n' "$c" >> "$OUT"; continue; }

  ident=$(echo "$c" | tr '-' '_')
  start=$(date +%s)
  timeout "$PER_MODULE_TIMEOUT" python3 scripts/lean_gap_report.py "$c" \
    --prefix "$ident::$mod::*" > "/tmp/gapsample-$c.log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if [ $rc -eq 124 ]; then
    printf '%s\t%s\ttimeout\t-\t-\t-\t%s\n' "$c" "$mod" "$secs" >> "$OUT"
  else
    row=$(python3 - "$c" <<'PY'
import json, sys
try:
    d = json.load(open(f"specs/lean/gaps/{sys.argv[1]}.json"))
except Exception:
    print("nojson - - -"); raise SystemExit
print(d.get("stage","?"), d.get("translation_errors","-"), d.get("lean_errors","-"),
      d.get("sorry_declarations","-"))
PY
)
    printf '%s\t%s\t%s\t%s\n' "$c" "$mod" "$row" "$secs" >> "$OUT"
  fi
  echo "[$(date +%H:%M:%S)] $c ($mod) -> $(tail -1 "$OUT" | cut -f3-)"
done
