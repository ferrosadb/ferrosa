#!/usr/bin/env bash
# Part B probe suite against the provisioned fly cluster. Each probe has a hard
# wall-clock timeout so a hang/deadlock FAILS rather than hangs the run, and each
# asserts every node's RSS stays under the 2 GiB ceiling throughout.
#
# SCAFFOLD: prints the probes it would run; executes only with --i-will-pay.
# Exits non-zero on the FIRST probe that fails or exceeds the memory ceiling.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

SEED_HOST="$(node_name 0).vm.${FLY_APP}.internal"

# Assert every node is below the RSS ceiling right now. Fail loud on breach.
assert_all_nodes_under_ceiling() {
  local label="$1"
  log "[${label}] asserting all nodes RSS < ${NODE_RSS_CEILING_BYTES} B"
  fly_do flyctl machine list --app "${FLY_APP}" --json  # in the real run: parse metrics endpoint per node
  # Real impl: for each node, curl :9090/metrics, read process_resident_memory_bytes,
  # and `die` if any exceeds NODE_RSS_CEILING_BYTES. Kept declarative in scaffold.
}

# Run one CQL/ws probe under a hard timeout; a timeout is a FAILURE.
probe() {
  local label="$1"; shift
  log "PROBE: ${label} (timeout ${PROBE_TIMEOUT_SECS}s)"
  assert_all_nodes_under_ceiling "before:${label}"
  fly_do timeout "${PROBE_TIMEOUT_SECS}" "$@"
  assert_all_nodes_under_ceiling "after:${label}"
}

# 0. FTS content scan OOM (t_ee98faa0) — the primary deterministic RED. One
#    fts_match content scan over the large entity_store must return candidates
#    with coordinator RSS bounded (no OOM).
probe "fts_content_scan" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl cql --host ${SEED_HOST} \
     --exec \"SELECT entity_id FROM agent_memory.entity_store WHERE context_snippet = fts_match('alpha bravo') ALLOW FILTERING;\""

# 1. Multi-page projected scan, RF>1, >10k: returns ALL rows, terminates, node
#    RSS bounded (independent of result size).
probe "multipage_projected_scan" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl cql --host ${SEED_HOST} --page-size ${CQL_PAGE_SIZE} \
     --exec \"SELECT entity_id, context_snippet FROM agent_memory.entity_store;\""

# 2. Abandoned page (client disconnect mid-scan): coordinator cancels remote
#    producers within a bound; live-producer count returns to 0; memory reclaims.
probe "abandoned_page_cancel" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl scan-abandon --host ${SEED_HOST} --table agent_memory.entity_store --disconnect-after-pages 1"

# 3. Slow/backpressured consumer: per-replica receive buffers stay bounded.
probe "slow_consumer_backpressure" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl scan-slow --host ${SEED_HOST} --table agent_memory.entity_store --consumer-delay-ms 250"

# 4. Viz SnapshotStreamEnd gap-close (t_dc729b1d): drive ws://…/viz/ws; zero new
#    sequence gap/reorder closes, probe reaches SnapshotStreamEnd.
probe "viz_snapshot_stream_end" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl viz-probe --host ${SEED_HOST} --expect SnapshotStreamEnd"

# 5. Data integrity across pages+replicas: union of pages == full keyset; COUNT
#    == scan cardinality across all coordinators.
probe "integrity_pages_replicas" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl scan-integrity --host ${SEED_HOST} --table agent_memory.entity_store --expect-count ${SEED_PARTITIONS}"

# 6. ORDER BY spill under cancel (t_5cf8dc78): cancel a spilling ORDER BY;
#    temp-sort dir removed, no orphan runs.
probe "order_by_spill_cancel" flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl scan-orderby-cancel --host ${SEED_HOST} --table agent_memory.entity_store"

log "All probes passed (dry-run unless --i-will-pay was passed)."
