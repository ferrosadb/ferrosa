#!/usr/bin/env bash
set -euo pipefail

# Cypher graph queries for the research knowledge graph tutorial.
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

echo "========================================"
echo "  Research Knowledge Graph Cypher Tests"
echo "  Host: ${FERROSA_GRAPH_HOST}:${FERROSA_GRAPH_PORT}"
echo "========================================"
echo ""

# ------------------------------------------------------------------
# Health and schema
# ------------------------------------------------------------------

run_query "Graph health check" "/graph/health"
run_query "Graph schema introspection" "/graph/schema?keyspace=knowledge"

echo ""
echo "--- Basic Node Queries ---"

# ------------------------------------------------------------------
# Basic node queries
# ------------------------------------------------------------------

run_query "Find researcher by name" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Alice Chen\"}) RETURN r.name, r.h_index, r.specializations", "keyspace": "knowledge"}'

run_query "All institutions ordered by ranking" \
    "/graph/query" "POST" \
    '{"query": "MATCH (i:Institution) RETURN i.name, i.country, i.ranking ORDER BY i.ranking", "keyspace": "knowledge"}'

run_query "Papers published in 2024" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) WHERE p.year = 2024 RETURN p.title, p.venue ORDER BY p.citation_count DESC", "keyspace": "knowledge"}'

run_query "Topics ordered by paper count" \
    "/graph/query" "POST" \
    '{"query": "MATCH (t:Topic) RETURN t.name, t.paper_count ORDER BY t.paper_count DESC", "keyspace": "knowledge"}'

run_query "Active grants with amounts" \
    "/graph/query" "POST" \
    '{"query": "MATCH (g:Grant) WHERE g.status = \"active\" RETURN g.title, g.funder, g.amount ORDER BY g.amount DESC", "keyspace": "knowledge"}'

echo ""
echo "--- Single-Hop Relationship Traversals ---"

# ------------------------------------------------------------------
# Single-hop relationship traversals
# ------------------------------------------------------------------

run_query "Papers authored by Alice Chen" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Alice Chen\"})-[:AUTHORED]->(p:Paper) RETURN p.title, p.year, p.venue ORDER BY p.year DESC", "keyspace": "knowledge"}'

run_query "Coauthors of Sparse Transformers paper" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AUTHORED]->(p:Paper {title: \"Attention Is All You Need 2.0: Sparse Transformers at Scale\"}) RETURN r.name, r.title ORDER BY r.name", "keyspace": "knowledge"}'

run_query "Researchers at MIT" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AFFILIATED_WITH]->(i:Institution {name: \"MIT\"}) RETURN r.name, r.h_index, r.title ORDER BY r.h_index DESC", "keyspace": "knowledge"}'

run_query "Papers citing Consensus Verification" \
    "/graph/query" "POST" \
    '{"query": "MATCH (citing:Paper)-[:CITES]->(p:Paper {title: \"Formal Verification of Distributed Consensus Protocols\"}) RETURN citing.title, citing.year", "keyspace": "knowledge"}'

run_query "Papers funded by DARPA" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:FUNDED_BY]->(g:Grant {funder: \"DARPA\"}) RETURN p.title, g.title AS grant_title", "keyspace": "knowledge"}'

run_query "Alice'\''s direct collaborators" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Alice Chen\"})-[:COLLABORATES]->(other:Researcher) RETURN other.name, other.title", "keyspace": "knowledge"}'

echo ""
echo "--- Multi-Hop Traversals ---"

# ------------------------------------------------------------------
# Multi-hop traversals
# ------------------------------------------------------------------

run_query "Citation chain (2 hops)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p1:Paper)-[:CITES]->(p2:Paper)-[:CITES]->(p3:Paper) RETURN p1.title AS citing, p2.title AS via, p3.title AS cited LIMIT 10", "keyspace": "knowledge"}'

run_query "Researcher topics via papers" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Alice Chen\"})-[:AUTHORED]->(p:Paper)-[:COVERS]->(t:Topic) RETURN DISTINCT t.name", "keyspace": "knowledge"}'

run_query "Cross-institution collaborations" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r1:Researcher)-[:AFFILIATED_WITH]->(i1:Institution), (r2:Researcher)-[:AFFILIATED_WITH]->(i2:Institution), (r1)-[:COLLABORATES]->(r2) WHERE i1.name <> i2.name RETURN r1.name, i1.name AS inst1, r2.name, i2.name AS inst2", "keyspace": "knowledge"}'

run_query "Funding chain: funder -> grant -> paper -> topic" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:FUNDED_BY]->(g:Grant), (p)-[:COVERS]->(t:Topic) RETURN g.funder, g.title, p.title, t.name ORDER BY g.funder", "keyspace": "knowledge"}'

run_query "Researcher -> paper -> citation -> coauthor" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Bob Martinez\"})-[:AUTHORED]->(p:Paper)-[:CITES]->(cited:Paper)<-[:AUTHORED]-(coauthor:Researcher) WHERE coauthor.name <> \"Bob Martinez\" RETURN DISTINCT coauthor.name, cited.title", "keyspace": "knowledge"}'

echo ""
echo "--- Aggregation Queries ---"

# ------------------------------------------------------------------
# Aggregation queries
# ------------------------------------------------------------------

run_query "Most cited papers (top 5)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) RETURN p.title, p.citation_count ORDER BY p.citation_count DESC LIMIT 5", "keyspace": "knowledge"}'

run_query "Papers per topic" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:COVERS]->(t:Topic) RETURN t.name, COUNT(p) AS paper_count ORDER BY paper_count DESC", "keyspace": "knowledge"}'

run_query "Researcher publication count" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AUTHORED]->(p:Paper) RETURN r.name, COUNT(p) AS publications ORDER BY publications DESC", "keyspace": "knowledge"}'

run_query "Collaboration count per researcher" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:COLLABORATES]->(other:Researcher) RETURN r.name, COUNT(other) AS collabs ORDER BY collabs DESC", "keyspace": "knowledge"}'

run_query "Average citations per venue" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) RETURN p.venue, AVG(p.citation_count) AS avg_citations, COUNT(p) AS papers ORDER BY avg_citations DESC", "keyspace": "knowledge"}'

run_query "Institution publication count" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AFFILIATED_WITH]->(i:Institution), (r)-[:AUTHORED]->(p:Paper) RETURN i.name, COUNT(DISTINCT p) AS papers ORDER BY papers DESC", "keyspace": "knowledge"}'

run_query "Grant impact (papers per grant)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (g:Grant)<-[:FUNDED_BY]-(p:Paper) RETURN g.title, g.funder, COUNT(p) AS papers ORDER BY papers DESC", "keyspace": "knowledge"}'

echo ""
echo "--- Path Queries ---"

# ------------------------------------------------------------------
# Path queries
# ------------------------------------------------------------------

run_query "Shortest path between Alice and Eva" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = shortestPath((a:Researcher {name: \"Alice Chen\"})-[*]-(b:Researcher {name: \"Eva Schmidt\"})) RETURN path", "keyspace": "knowledge"}'

run_query "All paths to a paper (max 3 hops)" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = (r:Researcher)-[*1..3]->(p:Paper {title: \"Attention Is All You Need 2.0: Sparse Transformers at Scale\"}) RETURN path LIMIT 10", "keyspace": "knowledge"}'

run_query "Shortest collaboration path (Alice to Jun)" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = shortestPath((a:Researcher {name: \"Alice Chen\"})-[:COLLABORATES*]-(b:Researcher {name: \"Jun Tanaka\"})) RETURN [n IN nodes(path) | n.name] AS chain", "keyspace": "knowledge"}'

run_query "Citation path depth 3" \
    "/graph/query" "POST" \
    '{"query": "MATCH path = (p:Paper)-[:CITES*1..3]->(end:Paper) WHERE p.title = \"Vision Transformers for Medical Image Segmentation\" RETURN [n IN nodes(path) | n.title] AS chain LIMIT 5", "keyspace": "knowledge"}'

echo ""
echo "--- Filtering and Predicates ---"

# ------------------------------------------------------------------
# Filtering and predicates
# ------------------------------------------------------------------

run_query "High h-index researchers (>40)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher) WHERE r.h_index > 40 RETURN r.name, r.h_index, r.title ORDER BY r.h_index DESC", "keyspace": "knowledge"}'

run_query "Highly cited papers (>100)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) WHERE p.citation_count > 100 RETURN p.title, p.citation_count, p.venue ORDER BY p.citation_count DESC", "keyspace": "knowledge"}'

run_query "Funded research with amounts" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper)-[:FUNDED_BY]->(g:Grant) RETURN p.title, g.funder, g.amount, g.currency ORDER BY g.amount DESC", "keyspace": "knowledge"}'

run_query "Papers with keyword filter" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) WHERE \"consensus\" IN p.keywords RETURN p.title, p.venue", "keyspace": "knowledge"}'

run_query "Researchers with specific specialization" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher) WHERE \"Distributed Systems\" IN r.specializations RETURN r.name, r.title, r.h_index", "keyspace": "knowledge"}'

run_query "Corresponding authors only" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[a:AUTHORED]->(p:Paper) WHERE a.corresponding = true RETURN r.name, p.title ORDER BY r.name", "keyspace": "knowledge"}'

echo ""
echo "--- Complex Graph Analytics ---"

# ------------------------------------------------------------------
# Complex graph analytics
# ------------------------------------------------------------------

run_query "Research influence (papers citing my papers)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Alice Chen\"})-[:AUTHORED]->(p:Paper)<-[:CITES]-(citing:Paper) RETURN p.title AS original, citing.title AS cited_by", "keyspace": "knowledge"}'

run_query "Collaboration network (collect)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:COLLABORATES]->(other:Researcher) RETURN r.name, COLLECT(other.name) AS collaborators ORDER BY SIZE(COLLECT(other.name)) DESC", "keyspace": "knowledge"}'

run_query "Topic overlap between MIT and Stanford" \
    "/graph/query" "POST" \
    '{"query": "MATCH (i1:Institution {name: \"MIT\"})<-[:AFFILIATED_WITH]-(r1:Researcher)-[:AUTHORED]->(p1:Paper)-[:COVERS]->(t:Topic)<-[:COVERS]-(p2:Paper)<-[:AUTHORED]-(r2:Researcher)-[:AFFILIATED_WITH]->(i2:Institution {name: \"Stanford\"}) RETURN DISTINCT t.name AS shared_topic", "keyspace": "knowledge"}'

run_query "Mutual citation detection" \
    "/graph/query" "POST" \
    '{"query": "MATCH (a:Paper)-[:CITES]->(b:Paper)-[:CITES]->(a) RETURN a.title, b.title", "keyspace": "knowledge"}'

run_query "Researcher reach (papers at distance 2)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher {name: \"Grace Okonkwo\"})-[:AUTHORED]->(p1:Paper)-[:CITES]->(p2:Paper) RETURN r.name, p1.title AS authored, p2.title AS influences", "keyspace": "knowledge"}'

run_query "Co-citation analysis" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p1:Paper)-[:CITES]->(common:Paper)<-[:CITES]-(p2:Paper) WHERE id(p1) < id(p2) RETURN p1.title, p2.title, common.title AS co_cited LIMIT 10", "keyspace": "knowledge"}'

run_query "Topic-spanning researchers" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AUTHORED]->(p:Paper)-[:COVERS]->(t:Topic) WITH r, COLLECT(DISTINCT t.name) AS topics WHERE SIZE(topics) >= 2 RETURN r.name, topics ORDER BY SIZE(topics) DESC", "keyspace": "knowledge"}'

run_query "Unfunded highly-cited papers" \
    "/graph/query" "POST" \
    '{"query": "MATCH (p:Paper) WHERE p.citation_count > 50 AND NOT (p)-[:FUNDED_BY]->(:Grant) RETURN p.title, p.citation_count ORDER BY p.citation_count DESC", "keyspace": "knowledge"}'

run_query "Institution collaboration graph" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r1:Researcher)-[:AFFILIATED_WITH]->(i1:Institution), (r2:Researcher)-[:AFFILIATED_WITH]->(i2:Institution), (r1)-[:COLLABORATES]->(r2) WHERE i1.name < i2.name RETURN i1.name, i2.name, COUNT(*) AS collaboration_count", "keyspace": "knowledge"}'

run_query "Topic expertise by institution" \
    "/graph/query" "POST" \
    '{"query": "MATCH (i:Institution)<-[:AFFILIATED_WITH]-(r:Researcher)-[:AUTHORED]->(p:Paper)-[:COVERS]->(t:Topic) RETURN i.name, t.name, COUNT(DISTINCT p) AS papers ORDER BY i.name, papers DESC", "keyspace": "knowledge"}'

echo ""
echo "--- WITH Chaining and Subqueries ---"

# ------------------------------------------------------------------
# WITH chaining and subqueries
# ------------------------------------------------------------------

run_query "Top researcher per institution" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[:AFFILIATED_WITH]->(i:Institution) WITH i, r ORDER BY r.h_index DESC WITH i, COLLECT(r)[0] AS top RETURN i.name, top.name, top.h_index", "keyspace": "knowledge"}'

run_query "Research pipeline: grant -> paper -> topic" \
    "/graph/query" "POST" \
    '{"query": "MATCH (g:Grant)<-[:FUNDED_BY]-(p:Paper)-[:COVERS]->(t:Topic) WITH g, COLLECT(DISTINCT t.name) AS topics, COUNT(DISTINCT p) AS papers RETURN g.title, g.funder, papers, topics", "keyspace": "knowledge"}'

run_query "Prolific collaborators (>= 2 collabs)" \
    "/graph/query" "POST" \
    '{"query": "MATCH (r:Researcher)-[c:COLLABORATES]->(other:Researcher) WITH r, COUNT(c) AS total_collabs WHERE total_collabs >= 2 RETURN r.name, total_collabs ORDER BY total_collabs DESC", "keyspace": "knowledge"}'

echo ""
echo "========================================"
echo "Results: ${PASS} passed, ${SKIP} skipped, ${FAIL} failed"
echo "========================================"

if [ "${FAIL}" -gt 0 ]; then
    exit 1
fi
