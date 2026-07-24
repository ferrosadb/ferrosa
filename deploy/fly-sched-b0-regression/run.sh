#!/usr/bin/env bash
# Run one arm's workload against an already-provisioned cluster and collect the
# evidence the T0.6 gate needs. Order is deliberate: SEED first (no sampling),
# then start the per-node samplers, then the SCAN STORM — so every sampled row
# is inside the storm window (no cross-host clock-skew windowing needed).
#
#   run.sh <arm-name> [--i-will-pay]      # arm-name: postfix | prefix
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARM="${1:?arm name required (postfix|prefix)}"; shift || true
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"
require_flyctl

ARM_OUT="${OUT_DIR}/${ARM}"
mkdir -p "${ARM_OUT}"
CLUSTER="$(all_node_cql_addrs)"
CLIENT="$(client_name)"
SCRAPE_SECS=$(( STORM_SECS + 30 ))

log "arm=${ARM}: cluster=[${CLUSTER}] client=${CLIENT}"
[ "${I_WILL_PAY}" -eq 1 ] && [ -z "${CLUSTER}" ] && die "no node IPs resolved — is the cluster up?"

# B2/B3 latency probe windows. The read_heavy probe OWNS load_test.data (its
# setup truncates, then its writers populate it) and its readers measure
# interactive point-read latency (SELECT val FROM data WHERE pk=? AND ck=1 — the
# fast path that bypasses the scheduler). The scan storm runs concurrently over
# the same table. We compare the read percentiles pre-B0 vs the full stack.
READ_LOAD_SECS="${READ_LOAD_SECS:-150}"   # read_heavy probe window
READ_RAMP_SECS="${READ_RAMP_SECS:-15}"    # let the probe populate before the storm

# ── Phase 1: start the interactive read-latency probe (owns the table) ─────────
log "read-latency probe (read_heavy, ${READ_LOAD_SECS}s) — populates + measures point-read latency"
fly_ssh "${CLIENT}" \
  "sh -c 'nohup ferrosa-loadgen --node \"${CLUSTER}\" --profile read_heavy --duration ${READ_LOAD_SECS} > /tmp/read.txt 2>&1 &'"
# Let the probe truncate + write initial keys so the storm has something to scan.
[ "${I_WILL_PAY}" -eq 1 ] && sleep "${READ_RAMP_SECS}"

# ── Phase 2: start per-node samplers (detached; survive the ssh session) ───────
# scrape.sh feeds the step-down gate; diag.sh gathers the freeze-hunt evidence
# (1s metrics + ~250ms thread states + an all-thread gdb backtrace on any >1s
# pause). diag runs long enough to cover the whole read-probe window.
DIAG_SECS="${DIAG_SECS:-$(( READ_LOAD_SECS + 20 ))}"
log "starting per-node samplers (scrape ${SCRAPE_SECS}s; diag ${DIAG_SECS}s with pause-triggered stack dumps)"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  fly_ssh "$(node_name "${i}")" \
    "sh -c 'nohup /usr/local/bin/scrape.sh /tmp/scrape.csv ${SAMPLE_INTERVAL_SECS} ${SCRAPE_SECS} >/tmp/scrape.log 2>&1 &'"
  fly_ssh "$(node_name "${i}")" \
    "sh -c 'nohup /usr/local/bin/diag.sh /tmp/metrics.csv /tmp/threads.log /tmp/stackdumps ${DIAG_SECS} >/tmp/diag.log 2>&1 &'"
done

# ── Phase 3: the scan storm (detached), concurrent with the read probe ─────────
log "SCAN STORM: ${SCAN_CONCURRENCY} workers x full-table ALLOW FILTERING for ${STORM_SECS}s (concurrent with the read probe)"
fly_ssh "${CLIENT}" \
  "sh -c 'nohup ferrosa-loadgen --node \"${CLUSTER}\" --scan-concurrency ${SCAN_CONCURRENCY} --scan-storm --duration ${STORM_SECS} > /tmp/scan.txt 2>&1 &'"

# ── Wait out the probe window, then collect both stdout streams ────────────────
if [ "${I_WILL_PAY}" -eq 1 ]; then
  remaining=$(( READ_LOAD_SECS - READ_RAMP_SECS + 10 ))
  log "waiting ${remaining}s for the read probe + storm to complete"
  sleep "${remaining}"
fi
fly_get "${CLIENT}" /tmp/read.txt "${ARM_OUT}/read-under-storm.stats.txt" || log "WARN could not fetch read probe output"
fly_get "${CLIENT}" /tmp/scan.txt "${ARM_OUT}/scan-storm.stats.txt" || log "WARN could not fetch scan-storm output"

# ── Phase 4: collect per-node evidence — scrape CSV, the COMPLETE teed ferrosa
#      log, and the diag bundle (metrics + thread states + pause stack dumps).
#      (Replaces the old `timeout flyctl logs` capture, which no-op'd on the
#      macOS harness host because `timeout` is absent, losing all node logs.) ────
for i in $(seq 0 $((NODE_COUNT - 1))); do
  name="$(node_name "${i}")"
  fly_get "${name}" /tmp/scrape.csv "${ARM_OUT}/scrape.node${i}.csv"
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    # Bundle everything on the node, then pull one tarball (reliable vs many gets).
    fly_ssh "${name}" \
      "sh -c 'tar czf /tmp/diag.tgz -C / tmp/metrics.csv tmp/threads.log tmp/stackdumps tmp/ferrosa.log tmp/diag.log 2>/dev/null; echo stackdumps:; ls -la /tmp/stackdumps 2>/dev/null | tail -20'" \
      > "${ARM_OUT}/diag.node${i}.listing.txt" 2>&1 || log "WARN tar/list on ${name} non-zero"
    fly_get "${name}" /tmp/diag.tgz "${ARM_OUT}/diag.node${i}.tgz" || log "WARN could not fetch diag bundle from ${name}"
    mkdir -p "${ARM_OUT}/node${i}"
    tar xzf "${ARM_OUT}/diag.node${i}.tgz" -C "${ARM_OUT}/node${i}" 2>/dev/null || log "WARN could not extract diag.node${i}.tgz"
  else
    log "DRY-RUN collect diagnostics (metrics/threads/stackdumps/ferrosa.log) for ${name}"
  fi
done

log "arm=${ARM} workload complete; evidence in ${ARM_OUT}"
