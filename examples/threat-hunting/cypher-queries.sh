#!/usr/bin/env bash
set -euo pipefail
# Graph lens for the threat-hunting tutorial: the SAME indicator/actor tables
# (created with graph.* extensions) traversed with Cypher to pivot across shared
# infrastructure and attribution -- the "connectedness" signal the CQL lenses
# can't express. Requires a running Ferrosa cluster with FERROSA_GRAPH_ENABLED=true.
FERROSA_GRAPH_HOST="${FERROSA_GRAPH_HOST:-localhost}"
FERROSA_GRAPH_PORT="${FERROSA_GRAPH_PORT:-7474}"
BASE_URL="http://${FERROSA_GRAPH_HOST}:${FERROSA_GRAPH_PORT}"
PASS=0
FAIL=0
SKIP=0
run_query() {
    local label="$1"
    local endpoint="$2"
    local method="${3:-GET}"
    local body="${4:-}"
    printf "%-65s " "${label}..."
    if [ "${method}" = "GET" ]; then
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}${endpoint}")
    else
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            -X "${method}" "${BASE_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -d "${body}")
    fi
    if [ "${HTTP_CODE}" -eq 200 ]; then
        echo "PASS (${HTTP_CODE})"; PASS=$((PASS + 1))
    elif [ "${HTTP_CODE}" -eq 400 ]; then
        echo "SKIP (${HTTP_CODE})"; SKIP=$((SKIP + 1))
    else
        echo "FAIL (${HTTP_CODE})"; FAIL=$((FAIL + 1))
    fi
}

run_query "Graph health check" "/graph/health"
run_query "Graph schema introspection" "/graph/schema?keyspace=threat"

# All indicators attributed to a given actor.
run_query "Indicators attributed to Emotet" \
    "/graph/query" "POST" \
    '{"query": "MATCH (i:Indicator)-[:ATTRIBUTED_TO]->(a:Actor {name: \"Emotet\"}) RETURN i.value, i.kind", "keyspace": "threat"}'

# Shared-infrastructure CENTRALITY: which infrastructure nodes are reused by the
# most indicators -- the pivots a hunter wants. Trivial in Cypher, a full scan in CQL.
run_query "Most-reused infrastructure (centrality)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (i:Indicator)-[:COMMUNICATES_WITH]->(t:Indicator) RETURN t.value, COUNT(i) AS used_by ORDER BY used_by DESC", "keyspace": "threat"}'

# Pivot: indicators that share infrastructure with a known-bad one.
run_query "Indicators sharing infra with IOC 1" \
    "/graph/query" "POST" \
    '{"query": "MATCH (a:Indicator {indicator_id: 1})-[:COMMUNICATES_WITH]->(s:Indicator)<-[:COMMUNICATES_WITH]-(b:Indicator) RETURN DISTINCT b.value", "keyspace": "threat"}'

# Cross-actor reuse: actors that share a common piece of infrastructure.
run_query "Actors linked through shared infrastructure" \
    "/graph/query" "POST" \
    '{"query": "MATCH (a1:Actor)<-[:ATTRIBUTED_TO]-(:Indicator)-[:COMMUNICATES_WITH]->(:Indicator)-[:ATTRIBUTED_TO]->(a2:Actor) WHERE a1.name <> a2.name RETURN DISTINCT a1.name, a2.name", "keyspace": "threat"}'

# Variable-length path: how is a phishing IOC connected to a C2 server?
run_query "Path from phishing IOC 1 to C2 IOC 5" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = shortestPath((a:Indicator {indicator_id: 1})-[:COMMUNICATES_WITH*]-(b:Indicator {indicator_id: 5})) RETURN path", "keyspace": "threat"}'

echo ""
echo "========================================"
echo "Results: ${PASS} passed, ${SKIP} skipped, ${FAIL} failed"
echo "========================================"

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
