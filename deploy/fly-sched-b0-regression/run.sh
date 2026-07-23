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

# ── Phase 1: seed the scan target (write_heavy fills load_test.data) ───────────
log "seeding scan target for ${SEED_SECS}s (write_heavy)"
fly_ssh "${CLIENT}" \
  "ferrosa-loadgen --node '${CLUSTER}' --profile write_heavy --duration ${SEED_SECS}" \
  > "${ARM_OUT}/seed.stats.txt" 2>&1 || log "WARN seed returned non-zero (see seed.stats.txt)"

# ── Phase 2: start per-node samplers (detached; survive the ssh session) ───────
log "starting per-node samplers (${SCRAPE_SECS}s @ ${SAMPLE_INTERVAL_SECS}s)"
for i in $(seq 0 $((NODE_COUNT - 1))); do
  fly_ssh "$(node_name "${i}")" \
    "sh -c 'nohup /usr/local/bin/scrape.sh /tmp/scrape.csv ${SAMPLE_INTERVAL_SECS} ${SCRAPE_SECS} >/tmp/scrape.log 2>&1 &'"
done
# A few baseline samples proving the leader is healthy BEFORE the storm.
[ "${I_WILL_PAY}" -eq 1 ] && sleep 8

# ── Phase 3: the scan storm ───────────────────────────────────────────────────
log "SCAN STORM: ${SCAN_CONCURRENCY} workers x full-table ALLOW FILTERING for ${STORM_SECS}s"
fly_ssh "${CLIENT}" \
  "ferrosa-loadgen --node '${CLUSTER}' --scan-storm --scan-concurrency ${SCAN_CONCURRENCY} --duration ${STORM_SECS}" \
  > "${ARM_OUT}/scan-storm.stats.txt" 2>&1 || log "WARN scan-storm returned non-zero (see scan-storm.stats.txt)"
[ "${I_WILL_PAY}" -eq 1 ] && sleep 5

# ── Phase 4: collect per-node sampler CSVs + a bounded log snapshot ────────────
for i in $(seq 0 $((NODE_COUNT - 1))); do
  name="$(node_name "${i}")"
  fly_get "${name}" /tmp/scrape.csv "${ARM_OUT}/scrape.node${i}.csv"
  if [ "${I_WILL_PAY}" -eq 1 ]; then
    id="$(machine_id_for_name "${name}")"
    # Bounded log snapshot (fly logs streams recent history then tails; cap it).
    timeout 25 flyctl logs --app "${FLY_APP}" --machine "${id}" > "${ARM_OUT}/logs.node${i}.txt" 2>/dev/null || true
  else
    log "DRY-RUN capture logs for ${name}"
  fi
done

log "arm=${ARM} workload complete; evidence in ${ARM_OUT}"
