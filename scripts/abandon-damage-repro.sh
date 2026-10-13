#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Canonical reproducer: "an abandoned COMMIT must leave no readable row".
#
#   scripts/abandon-damage-repro.sh            # one command, prints PASS/FAIL
#
# It runs ONE forced-failure case against the live 3-node Ferrosa bench
# cluster on Fly:
#
#     BEGIN;  COPY N rows;  COMMIT      <- forced to fail
#     then a POINT LOOKUP of each of the N known keys on EVERY node.
#
# MECHANISM (named): a whole-transaction write-set large enough in TOTAL BYTE
# VOLUME that the coordinator's Apply fan-out cannot reach its quorum inside
# the operator's 10 s bound.  The 10 s bound is a deliberate detector and is
# NOT tunable from this script.
#
# SIZE LAW, and the instrument defect this script used to have.  The bound
# responds to the write-set's BYTE VOLUME, so the volume must be reached with
# entries whose PER-ENTRY framed size stays UNDER the commit-log segment
# capacity (33554399 bytes usable; `commit_log.segment_size = 32 MiB`).  An
# entry larger than that is refused by the storage preflight in
# `write_atomic_batch` (ferrosa-storage/src/engine.rs) BEFORE anything is
# appended, so a write-set built from oversized entries can never be applied by
# the normal path -- and a run in that shape is not evidence of a partial apply
# at all.  Measured live on pgwire-eabee412: an autocommit COPY of ONE 40 MB row
# is refused with
#     `write failed: invalid data: batch entry (40000127 bytes) exceeds
#      commit-log segment capacity (33554399 bytes usable)`
# and leaves NO row on any node.  The earlier revision of this script used
# 3 x 40 MB -- entries OVER the cap -- so its forced failure was a PREFLIGHT
# REFUSAL and never exercised the apply path.  The default shape is therefore
# ROWS x ENTRY_BYTES with every entry comfortably under the cap.
#
#   Node-freezing was tried FIRST and is REFUTED on this rig: SIGSTOP of one
#   or both remote engine processes fails the Commit consensus outright with
#   `Accord quorum unavailable` in ~2.7 s -- it never reaches the Apply bound
#   -- and it additionally wedges that node's inter-node lanes.  See
#   specs/reference/abandon-damage-repro.md for the measurements.  A freeze
#   diagnostic is kept behind FORCE_MECHANISM=freeze.
#
# Four rules this script exists to enforce:
#   * NEVER assert on a COUNT.  A partial apply preserves a count and drops
#     the tail.  Every check is a point lookup of a known key.
#   * ALWAYS take the PER-NODE union.  A read served from one node's local
#     store is short by construction and looks exactly like lost data.
#   * NEVER claim damage from an UNVERIFIED START.  `drop table if exists` +
#     `create table` does NOT clear stored rows on this engine: the rows
#     survive and are readable again once the table is recreated under the same
#     name (measured: key 2 readable on node2 and key 3 on node3, both 40 MB,
#     straight through a drop/create).  So every key is point-looked-up on all
#     three nodes AFTER the drop/create and BEFORE the transaction; if any key
#     is readable the run prints NO-VERDICT and exits non-zero.  Never a damage
#     verdict from an unverified start.
#   * STATE THE PREFLIGHT OUTCOME.  A write-set that was REFUSED for capacity
#     appends nothing and can never be evidence of a partial apply, so a
#     refused run is reported as NO-VERDICT rather than as a result.  The
#     outcome is established DETERMINISTICALLY by a capacity control -- a
#     single-row autocommit COPY of the same value size, whose refusal the front
#     end hands back to the client verbatim -- never by the engine log, whose
#     buffer is small enough that a refusal can rotate out before it is read
#     (an earlier revision of this script mis-reported a REFUSED shape as
#     ACCTED for exactly that reason, and printed a damage verdict from it).
#
# Verdicts: PASS (abandon was clean) / FAIL (damage) / INADMISSIBLE (cold
# cluster or leftover client: no verdict is claimed) / NO-VERDICT (the forced
# commit did not actually fail, OR it failed at the CONSENSUS stage, OR the
# write-set was REFUSED at the storage preflight, OR the pre-condition was not
# established).
# ---------------------------------------------------------------------------
set -uo pipefail

CTRL_APP=${CTRL_APP:-maas-loadtest-control}
RUN_APP=${RUN_APP:-maas-loadtest-pgrunner}
RUNNER_ID=${RUNNER_ID:-48e01eebe961d8}
TABLE=${TABLE:-abandon_repro}
GUARD_TABLE=${GUARD_TABLE:-abandon_repro_guard}
# Per-row VALUE size.  The framed entry adds ~129 B (40 MB row -> 40000129 B
# for a 13-char table name), so 10 MB keeps every entry far under the 33554399 B
# segment cap: the preflight ACCEPTS and the bound, not the preflight, is what
# can fail.  ENTRY_BYTES over the cap reproduces only a refusal, never an apply.
ENTRY_BYTES=${ENTRY_BYTES:-${BIGBYTES:-10000000}}
# ROWS x ENTRY_BYTES is the total write-set.  12 x 10 MB = 120 MB, the same
# volume as the originally recorded case, reached with entries that fit.
ROWS=${ROWS:-12}
KEYS=$(seq 1 "$ROWS" | tr '\n' ' ')         # keys are 1..ROWS
CAP_BYTES=${CAP_BYTES:-33554399}           # observed usable segment cap; PRINTED for comparison, never asserted on
CAPCTL_TABLE=${CAPCTL_TABLE:-${TABLE}_capctl}   # scratch table for the capacity control
FORCE_MECHANISM=${FORCE_MECHANISM:-bulk}   # bulk | freeze
REMOTE=/tmp/abandon_repro_remote.sh

say(){ printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fatal(){ printf 'VERDICT: INADMISSIBLE  -- %s\n' "$*" >&2; exit 3; }

# NB: fly joins the argument list into a shell command line, so the inner
# command MUST be quoted as bash -lc '...' or only the first word reaches bash.
rex(){ case "$1" in *"'"*) echo "INTERNAL: rex command has a single quote" >&2; exit 3;; esac
       fly machine exec "$RUNNER_ID" -a "$RUN_APP" "bash -lc '$1'" 2>&1; }
nex(){ case "$2" in *"'"*) echo "INTERNAL: nex command has a single quote" >&2; exit 3;; esac
       fly machine exec "$1" -a "$CTRL_APP" "/bin/sh -c '$2'" 2>&1; }
poll(){ # $1 remote file  $2 marker  $3 timeout (poll iterations, ~4-5 s each)
  local i; for i in $(seq 1 "$3"); do
    rex "cat $1 2>/dev/null" | grep -q "$2" && return 0; sleep 3
  done; return 1
}
# Oversized-entry refusals visible in the ENGINE logs.  `fly logs` is an
# unbounded stream, so `--no-tail` makes it dump and exit and the `head` bound
# (BEFORE the filter) is what terminates the pipe.  This is CORROBORATION ONLY:
# the buffer is small and rotates, so a refusal can be missing even when it
# happened.  The verdict never depends on it -- section 4a's capacity control
# asks the engine directly.
refusal_newest(){ fly logs -a "$CTRL_APP" --no-tail 2>&1 | head -2000 \
              | grep 'exceeds commit-log segment capacity' | tail -1; }

# ---------------------------------------------------------------------------
# 0. Prerequisites.  Fail loud rather than produce a verdict about a cluster
#    that was not up.
# ---------------------------------------------------------------------------
say "preflight"
[ -n "$(fly auth whoami 2>/dev/null)" ] || fatal "fly CLI is not authenticated"

NODES_JSON=$(fly machine list -a "$CTRL_APP" --json 2>/dev/null)
[ -n "$NODES_JSON" ] || fatal "cannot list $CTRL_APP"
eval "$(printf '%s' "$NODES_JSON" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for m in d:
    k="IP_"+(m.get("name") or m["id"]).upper().replace("-","_")
    print("%s=%s"%(k,m.get("private_ip","")))
')"
IP_NODE1=${IP_NODE1:-}; IP_NODE2=${IP_NODE2:-}; IP_NODE3=${IP_NODE3:-}
[ -n "$IP_NODE1" ] && [ -n "$IP_NODE2" ] && [ -n "$IP_NODE3" ] \
  || fatal "could not resolve node1/node2/node3 private IPs"
printf '%s' "$NODES_JSON" | python3 -c '
import json,sys
d=json.load(sys.stdin)
bad=[m["name"] for m in d if m.get("state")!="started"]
sys.exit(0 if not bad else 1)
' || fatal "not all control nodes are 'started' -- refuse to measure"
say "  nodes: node1=$IP_NODE1 node2=$IP_NODE2 node3=$IP_NODE3"

rex "echo ok" >/dev/null 2>&1 || rex "echo ok" >/dev/null 2>&1

CLIENTS=$(rex "if pgrep -x psql >/dev/null 2>&1 || pgrep -x pgbench >/dev/null 2>&1; then echo CLIENTS_LEFT; else echo NO_CLIENTS; fi")
printf '%s' "$CLIENTS" | grep -q NO_CLIENTS \
  || fatal "a client is left over on the runner (psql/pgbench); clear it before measuring"
say "  runner has no leftover client"

# ---------------------------------------------------------------------------
# 1. Push the remote half (base64, so nothing is re-split by two shells).
# ---------------------------------------------------------------------------
REMOTE_SRC=$(cat <<'REMOTE'
#!/usr/bin/env bash
# Remote half of the abandon-damage reproducer (runs ON the pgrunner machine).
set -uo pipefail
PSQL=/usr/lib/postgresql/18/bin/psql
export PGPASSWORD="${F1_PASS:-}"
N1="$F1_CONN"
N2=$(printf '%s' "$F1_CONN" | sed "s|host=[^ ]*|host=${NODE2_IP:?}|")
N3=$(printf '%s' "$F1_CONN" | sed "s|host=[^ ]*|host=${NODE3_IP:?}|")
TAB="${TABLE:-abandon_repro}"
GTAB="${GUARD_TABLE:-abandon_repro_guard}"
CTAB="${CAPCTL_TABLE:-abandon_repro_capctl}"
BIG="${BIGBYTES:-10000000}"
ROWS="${ROWS:-12}"
KEYS=$(seq 1 "$ROWS" | tr '\n' ' ')

# one statement per call: a multi-statement -c is refused by this front end.
q(){ local o r; o=$(timeout 20 "$PSQL" "$1" -X -q -t -A -c "$2" 2>&1); r=$?
     printf 'rc=%s|%s' "$r" "$(printf '%s' "$o"|tr '\n' ' '|cut -c1-260)"; }
ddl(){ local c t; for t in 1 2 3 4 5; do c=$(q "$N1" "$2"); case "$c" in rc=0*) break;; esac; sleep 5; done;
     echo "$1: $c"; }

case "${1:-}" in
guard)
  # COLD-CLUSTER GUARD.  A control key written by a normal COMMIT must be
  # readable on every node.  `read timeout ... data_present=false` is the
  # named cold-cluster signature: report it and stop.
  : > /tmp/ar.guard.out
  ddl "guard_drop"   "drop table if exists $GTAB"              >> /tmp/ar.guard.out
  ddl "guard_create" "create table $GTAB (a int primary key, b text)" >> /tmp/ar.guard.out
  { echo "guard_write: $(q "$N1" "insert into $GTAB (a,b) values (7,'CTRL')")"
    for p in "node1:$N1" "node2:$N2" "node3:$N3"; do
      echo "guard_read ${p%%:*}: $(q "${p#*:}" "select b from $GTAB where a = 7")"
    done; } >> /tmp/ar.guard.out
  ;;
prepare)
  # VERIFIED PRE-CONDITION.  drop + create, then point-look up EVERY key on
  # every node and require ABSENT.  A drop/create on this engine does not clear
  # stored rows: they are readable again as soon as the table is recreated under
  # the same name, so a readable key here is a LEFTOVER from an earlier run, and
  # a later read of it would be indistinguishable from damage caused by this run.
  : > /tmp/ar.pre.out
  ddl "drop"   "drop table if exists $TAB"                       >> /tmp/ar.pre.out
  ddl "create" "create table $TAB (a int primary key, b text)"   >> /tmp/ar.pre.out
  for k in $KEYS; do
    for p in "node1:$N1" "node2:$N2" "node3:$N3"; do
      echo "pre key $k ${p%%:*}: $(q "${p#*:}" "select a from $TAB where a = $k")" >> /tmp/ar.pre.out
    done
  done
  echo "PRE_DONE" >> /tmp/ar.pre.out
  ;;
capctl)
  # CAPACITY CONTROL.  Can ONE entry of THIS shape's per-row size be applied by
  # the NORMAL (committing) path?  A single-row autocommit COPY of the same
  # VALUE size goes through the same commit-log preflight, and the front end
  # hands the client the refusal verbatim, so the outcome is deterministic and
  # client-visible -- no reliance on volatile engine logs.  A REFUSED shape can
  # never test the apply path, whatever the abandon message says.
  : > /tmp/ar.capctl.out
  ddl "capctl_drop"   "drop table if exists $CTAB"                >> /tmp/ar.capctl.out
  ddl "capctl_create" "create table $CTAB (a int primary key, b text)" >> /tmp/ar.capctl.out
  head -c "$BIG" /dev/zero | tr '\0' 'x' > /tmp/ar.capctl.row
  { printf 'copy %s from stdin;\n' "$CTAB"
    printf '0\t'; cat /tmp/ar.capctl.row; printf '\n\\.\n'; } > /tmp/ar.capctl.sql
  "$PSQL" "$N1" -X -q -v ON_ERROR_STOP=1 -f /tmp/ar.capctl.sql > /tmp/ar.capctl.client 2>&1
  echo "capctl_rc=$?" >> /tmp/ar.capctl.out
  sed 's/^/client: /' /tmp/ar.capctl.client >> /tmp/ar.capctl.out
  echo "CAPCTL_DONE" >> /tmp/ar.capctl.out
  ;;
run)
  # The forced-failure case: ONE transaction, ROWS keys, COMMIT forced to fail
  # by a write-set whose Apply fan-out cannot reach quorum inside 10 s.  Every
  # entry is under the segment cap, so the storage preflight ACCEPTS the set and
  # the 10 s bound is what fails -- the only shape that tests the invariant.
  : > /tmp/ar.run.out
  : > /tmp/ar.rows
  for k in $KEYS; do printf '%s\t' "$k" >> /tmp/ar.rows
                    head -c "$BIG" /dev/zero | tr '\0' 'x' >> /tmp/ar.rows
                    printf '\n' >> /tmp/ar.rows; done
  echo "write_set_bytes: $(wc -c < /tmp/ar.rows)" >> /tmp/ar.run.out
  { printf 'begin;\n'
    printf 'copy %s from stdin;\n' "$TAB"
    cat /tmp/ar.rows
    printf '\\.\n'
    printf 'commit;\n'; } > /tmp/ar.txn.sql
  t0=$(date +%s)
  "$PSQL" "$N1" -X -q -v ON_ERROR_STOP=1 -f /tmp/ar.txn.sql > /tmp/ar.client 2>&1
  rc=$?; t1=$(date +%s)
  echo "txn_rc=$rc elapsed=$((t1-t0))s" >> /tmp/ar.run.out
  sed 's/^/client: /' /tmp/ar.client >> /tmp/ar.run.out
  echo "RUN_DONE" >> /tmp/ar.run.out
  ;;
readkeys)
  # PER-NODE point lookup of every key.  Never a count.
  : > /tmp/ar.read.out
  for k in $KEYS; do
    for p in "node1:$N1" "node2:$N2" "node3:$N3"; do
      echo "key $k ${p%%:*}: $(q "${p#*:}" "select a from $TAB where a = $k")" >> /tmp/ar.read.out
    done
  done
  echo "READ_DONE" >> /tmp/ar.read.out
  ;;
cleanup)
  pkill -x psql 2>/dev/null
  rm -f /tmp/ar.rows /tmp/ar.txn.sql
  ;;
esac
REMOTE
)
printf '%s' "$REMOTE_SRC" | gzip -9 | base64 | tr -d '\n' > /tmp/ar.remote.b64
LB64=$(cat /tmp/ar.remote.b64)
rex "echo $LB64 | base64 -d | gunzip > $REMOTE && chmod +x $REMOTE && md5sum $REMOTE" >/tmp/ar.push.txt 2>&1
RHASH=$(grep -oE '[0-9a-f]{32}' /tmp/ar.push.txt | tail -1)
if command -v md5 >/dev/null 2>&1; then LHASH=$(printf '%s' "$REMOTE_SRC" | md5 -q)
else LHASH=$(printf '%s' "$REMOTE_SRC" | md5sum | cut -d' ' -f1); fi
[ "$RHASH" = "$LHASH" ] || fatal "remote helper failed to land (hash $RHASH != $LHASH)"
say "  remote helper verified (md5 $LHASH)"

RENV="env NODE2_IP=$IP_NODE2 NODE3_IP=$IP_NODE3 TABLE=$TABLE GUARD_TABLE=$GUARD_TABLE CAPCTL_TABLE=$CAPCTL_TABLE BIGBYTES=$ENTRY_BYTES ROWS=$ROWS"

# ---------------------------------------------------------------------------
# 2. Cold-cluster guard.
# ---------------------------------------------------------------------------
say "guard: control write + per-node control read"
rex "rm -f /tmp/ar.guard.out; setsid nohup $RENV $REMOTE guard > /tmp/ar.guard.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.guard.out guard_read 60 || fatal "guard step did not complete"
GUARD=$(rex "cat /tmp/ar.guard.out")
printf '%s\n' "$GUARD" | sed 's/^/  /'
printf '%s' "$GUARD" | grep -q 'read timeout.*data_present=false' && \
  fatal "cold cluster: control read returned 'read timeout ... data_present=false'"
printf '%s' "$GUARD" | grep -q 'guard_write: rc=0' \
  || fatal "control write did not commit"
[ "$(printf '%s' "$GUARD" | grep -c 'guard_read node[123]: rc=0|CTRL')" = 3 ] \
  || fatal "control read was not served by all three nodes"
say "  cluster warm: control key served by all three nodes"

# ---------------------------------------------------------------------------
# 3. VERIFIED PRE-CONDITION.  drop/create, then require every key ABSENT on all
#    three nodes BEFORE the transaction.  No damage verdict from an unverified
#    start: a drop/create on this engine leaves earlier rows readable.
# ---------------------------------------------------------------------------
say "pre-condition: drop/create $TABLE, then require keys ABSENT on all three nodes"
rex "rm -f /tmp/ar.pre.out; setsid nohup $RENV $REMOTE prepare > /tmp/ar.pre.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.pre.out PRE_DONE 90 || fatal "pre-condition step did not complete"
PRE=$(rex "cat /tmp/ar.pre.out")
printf '%s\n' "$PRE" | sed 's/^/  /'
printf '%s' "$PRE" | grep -q 'create: rc=0' || fatal "could not create $TABLE"
CONTAMINATED=$(printf '%s' "$PRE" | grep -E 'pre key [0-9]+ node[123]: rc=0[|][0-9]' || true)
if [ -n "$CONTAMINATED" ]; then
  printf '\nVERDICT: NO-VERDICT  -- the PRE-CONDITION was not established\n'
  printf '  these keys are readable BEFORE the transaction.  drop table + create table did\n'
  printf '  NOT clear them (they are leftovers from an earlier run, resurrected by the\n'
  printf '  recreated table), so any post-transaction read of them would be unreadable as\n'
  printf '  evidence -- a damage verdict here is not claimed:\n'
  printf '%s\n' "$CONTAMINATED" | sed 's/^/    /'
  exit 5
fi
say "  precondition holds: all $ROWS keys absent on node1, node2 and node3"

# ---------------------------------------------------------------------------
# 4a. CAPACITY CONTROL -- state the mechanism DETERMINISTICALLY.  The node log
#     is not a safe oracle: `fly logs` returns a small rotating buffer, so an
#     earlier revision missed a refusal that had already rotated out and
#     reported a REFUSED shape as ACCTED.  The control below asks the engine
#     itself, and the front end hands the client the refusal verbatim.
# ---------------------------------------------------------------------------
ENTRY_EST=$(( ENTRY_BYTES + 129 ))
say "capacity control: can ONE entry of $ENTRY_BYTES B be applied by the normal path?"
rex "rm -f /tmp/ar.capctl.out; setsid nohup $RENV $REMOTE capctl > /tmp/ar.capctl.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.capctl.out CAPCTL_DONE 90 || fatal "the capacity control did not complete"
CAPCTL=$(rex "cat /tmp/ar.capctl.out")
printf '%s\n' "$CAPCTL" | grep -E 'capctl_drop|capctl_create|capctl_rc|client:' | sed 's/^/  /'
if printf '%s' "$CAPCTL" | grep -q 'exceeds commit-log segment capacity'; then
  REFUSED=1
elif printf '%s' "$CAPCTL" | grep -q 'capctl_rc=0'; then
  REFUSED=0
else
  REFUSED=unknown
fi
case "$REFUSED" in
  1) say "  PREFLIGHT: REFUSED for capacity -- an entry of this size CANNOT be applied, so this shape cannot test the apply path" ;;
  0) say "  PREFLIGHT: ACCTED -- one entry of this size commits, so the write-set CAN be applied" ;;
  *) say "  PREFLIGHT: UNKNOWN -- the control neither committed nor showed a capacity refusal; no verdict" ;;
esac

# ---------------------------------------------------------------------------
# 4b. The forced-failure case.
# ---------------------------------------------------------------------------
say "run: BEGIN; COPY $ROWS rows ($ENTRY_BYTES B each); COMMIT  (mechanism=$FORCE_MECHANISM)"
say "  shape: entry_est=${ENTRY_EST}B vs cap=${CAP_BYTES}B usable; write_set_est=$(( ROWS * ENTRY_BYTES ))B"
rex "rm -f /tmp/ar.run.out; setsid nohup $RENV $REMOTE run > /tmp/ar.run.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.run.out RUN_DONE 120 || fatal "the forced-failure case did not complete"
RUN=$(rex "cat /tmp/ar.run.out")
printf '%s\n' "$RUN" | grep -E 'write_set_bytes|txn_rc|client:' | sed 's/^/  /'
# Corroboration only (the buffer may already have rotated this run's line out);
# the verdict NEVER depends on it.
CORROB=$(refusal_newest)
[ -n "$CORROB" ] && printf '  corroboration (node log, may be stale): %s\n' "$CORROB"

if printf '%s' "$RUN" | grep -qi 'abandoned and NOT committed'; then
  say "  the COMMIT was ABANDONED (the forced failure landed)"
  ABANDONED=1
elif printf '%s' "$RUN" | grep -qi 'quorum unavailable'; then
  printf 'VERDICT: NO-VERDICT  -- the commit was rejected at the CONSENSUS stage\n'
  printf '  (Accord quorum unavailable), not at the Apply bound.  The forced failure did not land.\n'
  exit 4
else
  printf 'VERDICT: NO-VERDICT  -- the COMMIT did not fail; the mechanism did not trip the bound\n'
  exit 4
fi

# ---------------------------------------------------------------------------
# 5. Per-node union of a point lookup on every known key.
# ---------------------------------------------------------------------------
say "read: per-node point lookup of $ROWS keys"
rex "rm -f /tmp/ar.read.out; setsid nohup $RENV $REMOTE readkeys > /tmp/ar.read.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.read.out READ_DONE 120 || fatal "per-node reads did not complete"
READ=$(rex "cat /tmp/ar.read.out")
printf '%s\n' "$READ" | sed 's/^/  /'

# ---------------------------------------------------------------------------
# 6. Verdict.
# ---------------------------------------------------------------------------
PRESENT=$(printf '%s' "$READ" | grep -E 'key [0-9]+ node[123]: rc=0[|][0-9]+' || true)
DANGLING=$(printf '%s' "$READ" | grep -E 'one silent source|fragment-merge|data_present=false|partial fan-out' || true)
CLIENTS=$(rex "if pgrep -x psql >/dev/null 2>&1 || pgrep -x pgbench >/dev/null 2>&1; then echo CLIENTS_LEFT; else echo NO_CLIENTS; fi")
printf '%s' "$CLIENTS" | grep -q NO_CLIENTS \
  || fatal "a client was left behind by this run; clean it up before the next"

# A REFUSED write-set appends nothing, so a run in that shape is not evidence of
# an apply-path partial apply.  It is reported as such -- but the reads above are
# still taken and any readable row is flagged, because a row that survives a
# refusal cannot have come from the normal apply path at all.
if [ "$REFUSED" = unknown ]; then
  printf '\nVERDICT: NO-VERDICT  -- the preflight outcome could not be established\n'
  printf '  The capacity control neither committed nor showed a capacity refusal, so it is\n'
  printf '  not known whether this write-set could be applied.  No verdict is claimed.\n'
  exit 6
fi
if [ "$REFUSED" = 1 ]; then
  printf '\nVERDICT: NO-VERDICT  -- the write-set was REFUSED at the storage preflight\n'
  printf '  The capacity control shows an entry of this size cannot be applied at all, so a\n'
  printf '  run in this shape cannot be evidence of a partial apply.  Re-run with every entry\n'
  printf '  under the %s B usable cap.\n' "$CAP_BYTES"
  printf '  Control refusal: %s\n' "$(printf '%s' "$CAPCTL" | grep 'exceeds commit-log segment capacity' | tail -1)"
  if [ -n "$PRESENT" ]; then
    printf '  ANOMALY (a separate finding, NOT an apply-path partial apply): rows are readable\n'
    printf '  after a retryable abandon even though the refused apply appended nothing:\n'
    printf '%s\n' "$PRESENT" | sed 's/^/    /'
  else
    printf '  Nothing is readable, which is the expected outcome of a refused apply.\n'
  fi
  exit 6
fi

if [ -n "$PRESENT" ]; then
  printf '\nVERDICT: FAIL  -- DAMAGE REPRODUCED\n'
  printf '  the client was told the transaction was NOT committed and was safe to retry,\n'
  printf '  yet these rows are readable now:\n'
  printf '%s\n' "$PRESENT" | sed 's/^/    /'
  printf '  per-node union is non-empty: a retry would double-apply.\n'
  exit 2
fi
if [ -n "$DANGLING" ]; then
  printf '\nVERDICT: FAIL  -- DAMAGE REPRODUCED (dangling read)\n'
  printf '%s\n' "$DANGLING" | sed 's/^/    /'
  exit 2
fi
printf '\nVERDICT: PASS  -- the abandon was clean\n'
printf '  the preflight ACCEPTED the write-set, the COMMIT was abandoned, and every one of\n'
printf '  the %s keys is absent on all three nodes afterwards.\n' "$ROWS"
exit 0
