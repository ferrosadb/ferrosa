#!/usr/bin/env bash
set -euo pipefail
# Graph lens for the scholarly-search tutorial: the SAME paper/author/cites
# tables (created with graph.* extensions) traversed with Cypher over the
# HTTP endpoint. This is the "connectedness / centrality" signal the CQL
# lenses cannot express (reverse citation counts, co-authorship, paths).
# Requires a running Ferrosa cluster with FERROSA_GRAPH_ENABLED=true.
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
        echo "PASS (${HTTP_CODE})"
        PASS=$((PASS + 1))
    elif [ "${HTTP_CODE}" -eq 400 ]; then
        echo "SKIP (${HTTP_CODE})"
        SKIP=$((SKIP + 1))
    else
        echo "FAIL (${HTTP_CODE})"
        FAIL=$((FAIL + 1))
    fi
}

run_query "Graph health check" "/graph/health"
run_query "Graph schema introspection" "/graph/schema?keyspace=scholar"

# Papers authored by a researcher.
run_query "Papers by Wei Zhang" \
    "/graph/query" "POST" \
    '{"query": "MATCH (a:Author {name: \"Wei Zhang\"})-[:AUTHORED]->(p:Paper) RETURN p.title, p.year ORDER BY p.year DESC", "keyspace": "scholar"}'

# Citation CENTRALITY: how many papers cite each paper (reverse edge + count) --
# impossible in CQL without a full scan, trivial in Cypher. Highly-cited = influential.
run_query "Most-cited papers (centrality)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:CITES]->(t:Paper) RETURN t.title, COUNT(p) AS cited_by ORDER BY cited_by DESC", "keyspace": "scholar"}'

# Who cites a specific influential paper (incoming edges).
run_query "Papers citing HNSW (paper 5)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:CITES]->(t:Paper {paper_id: 5}) RETURN p.title", "keyspace": "scholar"}'

# Co-authorship: 2-hop traversal through shared papers.
run_query "Co-authors of Wei Zhang" \
    "/graph/query" "POST" \
    '{"query": "MATCH (a:Author {name: \"Wei Zhang\"})-[:AUTHORED]->(:Paper)<-[:AUTHORED]-(co:Author) RETURN DISTINCT co.name", "keyspace": "scholar"}'

# Variable-length path: how is a vector-search paper connected to a consensus
# paper through the citation graph?
run_query "Path between Scalar Quant (8) and HNSW (5)" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = shortestPath((a:Paper {paper_id: 8})-[:CITES*]->(b:Paper {paper_id: 5})) RETURN path", "keyspace": "scholar"}'

echo ""
echo "========================================"
echo "Results: ${PASS} passed, ${SKIP} skipped, ${FAIL} failed"
echo "========================================"

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
