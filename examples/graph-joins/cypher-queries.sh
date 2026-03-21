#!/usr/bin/env bash
set -euo pipefail

# Cypher graph queries for the social network tutorial.
# Requires a running Ferrosa cluster with FERROSA_GRAPH_ENABLED=true.

FERROSA_GRAPH_HOST="${FERROSA_GRAPH_HOST:-localhost}"
FERROSA_GRAPH_PORT="${FERROSA_GRAPH_PORT:-7474}"
BASE_URL="http://${FERROSA_GRAPH_HOST}:${FERROSA_GRAPH_PORT}"

PASS=0
FAIL=0

run_query() {
    local label="$1"
    local endpoint="$2"
    local method="${3:-GET}"
    local body="${4:-}"

    printf "%-60s " "${label}..."

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
    else
        echo "FAIL (${HTTP_CODE})"
        FAIL=$((FAIL + 1))
    fi
}

echo "========================================"
echo "Ferrosa Graph Cypher Query Tests"
echo "Host: ${FERROSA_GRAPH_HOST}:${FERROSA_GRAPH_PORT}"
echo "========================================"
echo ""

# Health and schema
run_query "Graph health check" "/graph/health"
run_query "Graph schema" "/graph/schema"

# Direct follows
run_query "Direct follows (Alice)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:FOLLOWS]->(b:Person) RETURN b.name, b.city"}'

# Friends of friends (2-hop traversal)
run_query "Friends of friends (Alice, 2-hop)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:FOLLOWS]->()-[:FOLLOWS]->(friend2:Person) WHERE friend2.name <> \"Alice\" RETURN DISTINCT friend2.name"}'

# Reverse traversal: who follows Alice?
run_query "Reverse traversal (who follows Alice)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (follower:Person)-[:FOLLOWS]->(a:Person {name: \"Alice\"}) RETURN follower.name"}'

# Coworkers at the same company
run_query "Coworkers (Alice at Acme Corp)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(coworker:Person) WHERE coworker.name <> \"Alice\" RETURN coworker.name, coworker.age, c.name AS company"}'

# Posts liked by people Alice follows
run_query "Posts liked by Alice'\''s friends" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:FOLLOWS]->(friend:Person)-[:LIKES]->(p:Post) RETURN friend.name, p.content"}'

# Shortest path between Alice and Hank
run_query "Shortest path (Alice to Hank)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH path = shortestPath((a:Person {name: \"Alice\"})-[:FOLLOWS*..6]->(h:Person {name: \"Hank\"})) RETURN [n IN nodes(path) | n.name] AS chain"}'

# Fraud ring detection: circular money flows
run_query "Fraud ring detection (transfer cycles)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH path = (a:Person)-[:TRANSFERS*2..4]->(a) RETURN [n IN nodes(path) | n.name] AS ring, [r IN relationships(path) | r.amount] AS amounts"}'

# Fraud ring context enrichment
run_query "Fraud ring context (suspect relationships)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (suspect:Person)-[:TRANSFERS*2..4]->(suspect) WITH DISTINCT suspect MATCH (suspect)-[r]-(connected) RETURN suspect.name, type(r) AS relationship, connected.name ORDER BY suspect.name"}'

# Collaborative filtering: post recommendations for Alice
run_query "Collaborative filtering (recommend posts)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:LIKES]->(p:Post)<-[:LIKES]-(other:Person)-[:LIKES]->(rec:Post) WHERE NOT (a)-[:LIKES]->(rec) RETURN rec.content, COUNT(other) AS score ORDER BY score DESC"}'

# Follow recommendations: people you may know
run_query "Follow recommendations (people you may know)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:FOLLOWS]->(friend)-[:FOLLOWS]->(suggestion:Person) WHERE NOT (a)-[:FOLLOWS]->(suggestion) AND suggestion.name <> \"Alice\" RETURN suggestion.name, suggestion.city, COUNT(friend) AS mutual_friends ORDER BY mutual_friends DESC"}'

# Company network discovery
run_query "Company network discovery (Alice)" \
    "/graph/query" "POST" \
    '{"keyspace": "social", "query": "MATCH (a:Person {name: \"Alice\"})-[:FOLLOWS*1..2]->(connection:Person)-[:WORKS_AT]->(c:Company) WHERE c.name <> \"Acme Corp\" RETURN c.name, c.industry, COLLECT(DISTINCT connection.name) AS connections"}'

echo ""
echo "========================================"
echo "Results: ${PASS} passed, ${FAIL} failed"
echo "========================================"

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
