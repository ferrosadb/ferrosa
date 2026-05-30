USE semantic;

-- 1) BTree secondary index: exact-match lookup on the scalar `category` column.
SELECT id, title FROM articles_hnsw WHERE category = 'science';

-- 2) HNSW vector index: approximate nearest-neighbour search.
--    The query vector sits in the science cluster, so the three science
--    articles (1, 5, 2) rank highest.
SELECT id, title FROM articles_hnsw
  ORDER BY embedding ANN OF [0.90, 0.10, 0.00, 0.00] LIMIT 3;

-- 3) HVQ vector index: the identical ANN query against the quantized index.
--    Results match the HNSW ranking while the index reads far fewer bytes.
SELECT id, title FROM articles_hvq
  ORDER BY embedding ANN OF [0.90, 0.10, 0.00, 0.00] LIMIT 3;

-- 4) A history-cluster query, to confirm both clusters are separable.
SELECT id, title FROM articles_hvq
  ORDER BY embedding ANN OF [0.00, 0.00, 0.90, 0.10] LIMIT 3;
