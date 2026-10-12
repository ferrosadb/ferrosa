#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Canonical reproducer: "an abandoned COMMIT must leave no readable row".
#
#   scripts/abandon-damage-repro.sh            # one command, prints PASS/FAIL
#
# It runs ONE forced-failure case against the live 3-node Ferrosa bench
# cluster on Fly:
#
#     BEGIN;  COPY 3 rows;  COMMIT      <- forced to fail
#     then a POINT LOOKUP of each of the 3 known keys on EVERY node.
#
# MECHANISM (named): a whole-transaction write-set large enough that the
# coordinator's Apply fan-out cannot reach its quorum inside the operator's
# 10 s bound.  That is the mechanism behind the recorded pgbench abandon (a
# one-transaction bulk load), here reproduced at THREE KEYS so the storage
# state is fully enumerable.  The 10 s bound is a deliberate detector and is
# NOT tunable from this script.
#
#   Node-freezing was tried FIRST and is REFUTED on this rig: SIGSTOP of one
#   or both remote engine processes fails the Commit consensus outright with
#   `Accord quorum unavailable` in ~2.7 s -- it never reaches the Apply bound
#   -- and it additionally wedges that node's inter-node lanes.  See
#   specs/reference/abandon-damage-repro.md for the measurements.  A freeze
#   diagnostic is kept behind FORCE_MECHANISM=freeze.
#
# Two rules this script exists to enforce:
#   * NEVER assert on a COUNT.  A partial apply preserves a count and drops
#     the tail.  Every check is a point lookup of a known key.
#   * ALWAYS take the PER-NODE union.  A read served from one node's local
#     store is short by construction and looks exactly like lost data.
#
# Verdicts: PASS (abandon was clean) / FAIL (damage) / INADMISSIBLE (cold
# cluster or leftover client: no verdict is claimed) / NO-VERDICT (the
# forced commit did not actually fail).
# ---------------------------------------------------------------------------
set -uo pipefail

CTRL_APP=${CTRL_APP:-maas-loadtest-control}
RUN_APP=${RUN_APP:-maas-loadtest-pgrunner}
RUNNER_ID=${RUNNER_ID:-48e01eebe961d8}
TABLE=${TABLE:-abandon_repro}
GUARD_TABLE=${GUARD_TABLE:-abandon_repro_guard}
BIGBYTES=${BIGBYTES:-40000000}     # per-row value size; 3 rows => 120 MB write-set
KEYS="1 2 3"
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
poll(){ # $1 remote file  $2 marker  $3 timeout secs
  local i; for i in $(seq 1 "$3"); do
    rex "cat $1 2>/dev/null" | grep -q "$2" && return 0; sleep 3
  done; return 1
}

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
BIG="${BIGBYTES:-40000000}"
KEYS="${KEYS:-1 2 3}"

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
run)
  # The forced-failure case: ONE transaction, THREE rows, COMMIT forced to
  # fail by a write-set whose Apply fan-out cannot reach quorum inside 10 s.
  : > /tmp/ar.run.out
  ddl "drop"   "drop table if exists $TAB"              >> /tmp/ar.run.out
  ddl "create" "create table $TAB (a int primary key, b text)" >> /tmp/ar.run.out
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

RENV="env NODE2_IP=$IP_NODE2 NODE3_IP=$IP_NODE3 TABLE=$TABLE GUARD_TABLE=$GUARD_TABLE BIGBYTES=$BIGBYTES"

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
# 3. The forced-failure case.
# ---------------------------------------------------------------------------
say "run: BEGIN; COPY $KEYS ($BIGBYTES B each); COMMIT  (mechanism=$FORCE_MECHANISM)"
rex "rm -f /tmp/ar.run.out; setsid nohup $RENV $REMOTE run > /tmp/ar.run.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.run.out RUN_DONE 120 || fatal "the forced-failure case did not complete"
RUN=$(rex "cat /tmp/ar.run.out")
printf '%s\n' "$RUN" | grep -E 'write_set_bytes|txn_rc|client:' | sed 's/^/  /'

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
# 4. Per-node union of a point lookup on every known key.
# ---------------------------------------------------------------------------
say "read: per-node point lookup of $KEYS"
rex "rm -f /tmp/ar.read.out; setsid nohup $RENV $REMOTE readkeys > /tmp/ar.read.stdout 2>&1 < /dev/null & echo LAUNCHED" >/dev/null 2>&1
poll /tmp/ar.read.out READ_DONE 120 || fatal "per-node reads did not complete"
READ=$(rex "cat /tmp/ar.read.out")
printf '%s\n' "$READ" | sed 's/^/  /'

# ---------------------------------------------------------------------------
# 5. Verdict.
# ---------------------------------------------------------------------------
PRESENT=$(printf '%s' "$READ" | grep -E 'key [0-9]+ node[123]: rc=0[|][0-9]+' || true)
DANGLING=$(printf '%s' "$READ" | grep -E 'one silent source|fragment-merge|data_present=false|partial fan-out' || true)
CLIENTS=$(rex "if pgrep -x psql >/dev/null 2>&1 || pgrep -x pgbench >/dev/null 2>&1; then echo CLIENTS_LEFT; else echo NO_CLIENTS; fi")
printf '%s' "$CLIENTS" | grep -q NO_CLIENTS \
  || fatal "a client was left behind by this run; clean it up before the next"

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
printf '  every one of the %s keys is absent on all three nodes after the abandoned COMMIT.\n' "$(printf '%s' "$KEYS" | wc -w | tr -d ' ')"
exit 0
