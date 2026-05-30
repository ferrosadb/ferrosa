-- Vector index strategies: BTree + HNSW + HVQ on the same data.
--
-- Ferrosa exposes three index strategies a user can reach from CQL:
--   1. A secondary BTree index for exact-match lookups on a scalar column.
--   2. A full-precision HNSW vector index (the default for USING 'vector').
--   3. A quantized HVQ vector index (USING 'vector' WITH OPTIONS={'method':'hvq'}),
--      which stores Q8/Q4 codes plus a staged exact-rerank tier for far less
--      storage and bytes-read per query at a small recall cost.
--
-- HNSW and HVQ are demonstrated on two identically-shaped tables so the same
-- ANN query can run against each.

CREATE KEYSPACE IF NOT EXISTS semantic
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
  };

USE semantic;

-- Full-precision HNSW table.
CREATE TABLE articles_hnsw (
    id        int PRIMARY KEY,
    category  text,
    title     text,
    embedding vector<float, 4>
);

-- Identically-shaped table whose vector index uses the quantized HVQ method.
CREATE TABLE articles_hvq (
    id        int PRIMARY KEY,
    category  text,
    title     text,
    embedding vector<float, 4>
);

-- 1) Secondary BTree index on a scalar column for exact-match lookups.
CREATE INDEX articles_category_idx ON articles_hnsw (category);

-- 2) Default vector index: full-precision HNSW navigable graph.
CREATE INDEX articles_hnsw_ann ON articles_hnsw (embedding) USING 'vector';

-- 3) Quantized vector index: hybrid vector quantization (Q8/Q4 + staged rerank).
CREATE INDEX articles_hvq_ann ON articles_hvq (embedding) USING 'vector'
    WITH OPTIONS = {'method': 'hvq'};
