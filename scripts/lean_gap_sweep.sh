#!/usr/bin/env bash
# Sweep the Lean gap report across every lib crate of the ferrosa workspace.
#
# One JSON per crate in specs/lean/gaps/ plus a roll-up line in $OUT. Each run is bounded so a
# crate that OOMs in charon or hangs in elaboration cannot stall the sweep.
set -uo pipefail
cd "$(dirname "$0")" >/dev/null 2>&1 || true
REPO=${REPO:-$HOME/src/ferrosa-suite/ferrosa}
OUT=${OUT:-/tmp/gapsweep.tsv}
PER_CRATE_TIMEOUT=${PER_CRATE_TIMEOUT:-900}

cd "$REPO" || exit 2
if [ "${RESUME:-0}" = "1" ] && [ -s "$OUT" ]; then
  echo "resuming from $OUT"
else
  : > "$OUT"
  printf 'crate\tstage\ttranslation_errors\tlean_errors\tsorry\tseconds\n' >> "$OUT"
fi

if [ "$#" -gt 0 ]; then
  crates="$*"
else
crates=$(
  python3 - <<'PY'
import json, subprocess
m = json.loads(subprocess.run(['cargo','metadata','--no-deps','--format-version','1'],
                              capture_output=True, text=True).stdout)
print(' '.join(sorted(p['name'] for p in m['packages']
                      if p['targets'] and any(t['kind'] == ['lib'] for t in p['targets']))))
PY
)
fi

for c in $crates; do
  if [ "${RESUME:-0}" = "1" ] && cut -f1 "$OUT" | grep -qx "$c"; then
    echo "skip $c (already measured)"
    continue
  fi
  start=$(date +%s)
  log=/tmp/gapsweep-$c.log
  timeout "$PER_CRATE_TIMEOUT" python3 scripts/lean_gap_report.py "$c" > "$log" 2>&1
  rc=$?
  secs=$(( $(date +%s) - start ))
  if [ $rc -eq 124 ]; then
    stage=timeout; te=-; le=-; so=-
  elif [ -f "specs/lean/gaps/$c.json" ]; then
    read -r stage te le so <<<"$(python3 - "$c" <<'PY'
import json, sys
try:
    d = json.load(open(f"specs/lean/gaps/{sys.argv[1]}.json"))
except Exception:
    print("nojson - - -"); raise SystemExit
def g(k, default="-"):
    v = d.get(k, default)
    return default if v is None else v
stage = g("stage")
if d.get("compiled"):
    stage = "compiled"
print(stage, g("translation_errors"), g("lean_errors"), g("sorry_declarations"))
PY
)"
  else
    stage=nojson; te=-; le=-; so=-
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$c" "$stage" "$te" "$le" "$so" "$secs" >> "$OUT"
  echo "[$(date +%H:%M:%S)] $c -> $stage (te=$te le=$le so=$so) ${secs}s"
done

echo "--- roll-up ---"
column -t -s $'\t' "$OUT" 2>/dev/null || cat "$OUT"
