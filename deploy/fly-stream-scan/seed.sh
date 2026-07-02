#!/usr/bin/env bash
# Seed the provisioned cluster: an `entity_store`-shaped table with a full-text
# indexed `context_snippet` column (>=50k partitions, >=3 pages) plus a
# `typed_edges`-shaped graph table for the viz probe.
#
# SCAFFOLD: prints the CQL it would run; executes it against a provisioned node
# only with --i-will-pay (which implies the machines already exist).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${HERE}/lib.sh" "$@"

SEED_HOST="$(node_name 0).vm.${FLY_APP}.internal"
log "Seeding via ${SEED_HOST}:9042 — ${SEED_PARTITIONS} partitions, ${SEED_EDGES} edges, snippet=${SEED_SNIPPET_BYTES}B"

# DDL mirrors agent_memory: entity_store with an FTI on context_snippet so a
# `WHERE context_snippet = fts_match(<terms>)` content scan runs; typed_edges
# for the viz SnapshotStreamEnd probe.
read -r -d '' SCHEMA <<CQL || true
CREATE KEYSPACE IF NOT EXISTS agent_memory
  WITH replication = {'class':'SimpleStrategy','replication_factor':${REPLICATION_FACTOR}};
CREATE TABLE IF NOT EXISTS agent_memory.entity_store (
  entity_id text PRIMARY KEY,
  context_snippet text
);
CREATE CUSTOM INDEX IF NOT EXISTS entity_store_snippet_fti
  ON agent_memory.entity_store (context_snippet) USING 'fulltext';
CREATE TABLE IF NOT EXISTS agent_memory.typed_edges (
  src text, edge_type text, dst text, observed_at timestamp,
  PRIMARY KEY (src, edge_type, dst)
);
CQL

log "Would apply schema:"
printf '%s\n' "${SCHEMA}" >&2

# The seeder itself is a small CQL loader; in the real run this is a
# `flyctl ssh console`-driven `ferrosa-ctl load` or a cqlsh loop. Kept as a
# documented invocation so the scaffold has no autonomous data write.
fly_do flyctl ssh console --app "${FLY_APP}" --command \
  "ferrosa-ctl seed --keyspace agent_memory --table entity_store \
     --partitions ${SEED_PARTITIONS} --snippet-bytes ${SEED_SNIPPET_BYTES} \
     --fts-terms 'alpha bravo charlie' \
   && ferrosa-ctl seed --keyspace agent_memory --table typed_edges --edges ${SEED_EDGES}"

log "Seed step complete (dry-run unless --i-will-pay was passed)."
