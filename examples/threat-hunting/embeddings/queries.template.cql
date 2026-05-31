-- GENERATED from embeddings/queries.template.cql by gen_embeddings.py.
-- ANN OF [...] vectors are real nomic-embed-text-v2-moe embeddings of the query
-- strings in corpus.json. Five lenses over the SAME indicator corpus.
USE threat;

-- 1) Keyword (lexical substring recall): IOC descriptions containing a phrase.
--    Precise when you know the tradecraft words, blind to paraphrase -- lens 2 fixes that.
SELECT indicator_id, value, kind FROM indicator
  WHERE description LIKE '%credential%' ALLOW FILTERING;

-- 2) Vector ANN over HVQ-quantized embeddings (semantic recall): indicators
--    whose BEHAVIOUR is closest to "fake bank login page that steals usernames
--    and passwords" -- surfaces the phishing cluster regardless of wording.
SELECT indicator_id, value, kind FROM indicator
  ORDER BY embedding ANN OF @@Q:ann_phishing@@ LIMIT 5;

-- 3) Phonetic (fuzzy actor recall): analyst typed "Lazerus", matches "Lazarus".
SELECT actor_id, name, origin FROM actor
  WHERE name SOUNDS LIKE 'Lazerus' ALLOW FILTERING;

-- 4) RRD trend signal (activity spike). The 5-minute rollup
--    indicator_activity_5m materializes asynchronously; the hunt reads the
--    always-populated source directly to find accelerating indicators.
SELECT indicator_id, minute, hits FROM indicator_activity WHERE indicator_id = 4;
SELECT indicator_id, minute, hits_sum, hits_max
  FROM indicator_activity_hourly WHERE indicator_id = 4;

-- 5) Graph lens as plain CQL: infrastructure an indicator talks to (outgoing
--    edges by partition key). Pivoting across actors / shared infrastructure is
--    via Cypher -- see cypher-queries.sh.
SELECT dst_indicator_id FROM communicates_with WHERE src_indicator_id = 4;

-- A contrasting semantic query: "command and control beacon for ransomware
-- encryption tasking" should surface the C2 cluster, not the phishing IOCs.
SELECT indicator_id, value FROM indicator
  ORDER BY embedding ANN OF @@Q:ann_c2@@ LIMIT 3;
