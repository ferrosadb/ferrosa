-- GENERATED from embeddings/queries.template.cql by gen_embeddings.py.
-- The ANN OF [...] vectors are real nomic-embed-text-v2-moe embeddings of the
-- query strings in corpus.json. Five lenses over the SAME paper corpus.
USE scholar;

-- 1) Keyword (lexical substring recall): abstracts containing a phrase. Precise
--    when you know the words, blind to synonyms -- which is what lens 2 fixes.
SELECT paper_id, title FROM paper
  WHERE abstract LIKE '%nearest neighbor%' ALLOW FILTERING;

-- 2) Vector ANN over HVQ-quantized embeddings (semantic recall): nearest papers
--    to the embedding of "memory-efficient approximate nearest neighbor search
--    over embeddings". Surfaces the vector-search cluster, even without keyword overlap.
SELECT paper_id, title, year FROM paper
  ORDER BY embedding ANN OF @@Q:ann_efficient_nn@@ LIMIT 5;

-- 3) Phonetic (fuzzy author recall): "Jon Smyth" matches stored "John Smith".
SELECT author_id, name, affiliation FROM author
  WHERE name SOUNDS LIKE 'Jon Smyth' ALLOW FILTERING;

-- 4) RRD trend signal (recent citation velocity). The weekly rollup
--    paper_citations_weekly materializes asynchronously from the consolidation
--    config; the rerank reads the always-populated daily source directly.
SELECT paper_id, day, citations FROM paper_citations_daily WHERE paper_id = 8;
SELECT paper_id, day, citations_sum, citations_max, citations_avg
  FROM paper_citations_weekly WHERE paper_id = 8;

-- 5) Graph lens as plain CQL: papers that paper 8 cites (outgoing edges by
--    partition key). Reverse traversal / centrality is via Cypher -- see
--    cypher-queries.sh -- which is what makes the graph lens worth it.
SELECT cited_paper_id FROM cites WHERE citing_paper_id = 8;

-- A contrasting semantic query: "agreement among replicas despite failures"
-- should surface the CONSENSUS cluster, not the vector-search papers --
-- demonstrating that ANN matches meaning, not words.
SELECT paper_id, title FROM paper
  ORDER BY embedding ANN OF @@Q:ann_consensus@@ LIMIT 3;
