-- Scholarly multi-model search: one corpus of papers, queried five ways.
--   * keyword (LIKE)     on abstract        -> WHERE abstract LIKE '%...%'
--   * vector ANN (HVQ)   on embedding        -> USING 'vector'    + ORDER BY ... ANN OF
--   * phonetic (Metaphone) on author name     -> USING 'phonetic'  + SOUNDS LIKE
--   * RRD consolidation  on citation counts   -> consolidation.*    (trend signal)
--   * property graph     on the same tables   -> graph.*            (Cypher centrality)
-- See README / scholarly-search.adoc for the reciprocal-rank-fusion rerank.

CREATE KEYSPACE IF NOT EXISTS scholar
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE scholar;

-- Papers: full-text searchable abstract, a real 768-d embedding (nomic-embed-text-v2-moe),
-- and a graph vertex so Cypher can traverse citations.
CREATE TABLE IF NOT EXISTS paper (
    paper_id  int PRIMARY KEY,
    title     text,
    abstract  text,
    year      int,
    venue     text,
    embedding vector<float, 768>
) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Paper' };

CREATE INDEX IF NOT EXISTS paper_ann ON paper (embedding) USING 'vector'
    WITH OPTIONS = {'method': 'hvq'};

-- Authors: phonetic name search (find "John Smith" from "Jon Smyth"); graph vertex.
CREATE TABLE IF NOT EXISTS author (
    author_id   int PRIMARY KEY,
    name        text,
    affiliation text
) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Author' };

CREATE INDEX IF NOT EXISTS author_phonetic ON author (name) USING 'phonetic';

-- Authorship edges (Author)-[:AUTHORED]->(Paper).
CREATE TABLE IF NOT EXISTS authored (
    author_id       int,
    paper_id        int,
    author_position int,
    PRIMARY KEY (author_id, paper_id)
) WITH extensions = {
    'graph.type': 'edge', 'graph.label': 'AUTHORED',
    'graph.source': 'author_id', 'graph.target': 'paper_id',
    'graph.source_label': 'Author', 'graph.target_label': 'Paper'
};

-- Citation edges (Paper)-[:CITES]->(Paper): centrality = influence.
CREATE TABLE IF NOT EXISTS cites (
    citing_paper_id int,
    cited_paper_id  int,
    PRIMARY KEY (citing_paper_id, cited_paper_id)
) WITH extensions = {
    'graph.type': 'edge', 'graph.label': 'CITES',
    'graph.source': 'citing_paper_id', 'graph.target': 'cited_paper_id',
    'graph.source_label': 'Paper', 'graph.target_label': 'Paper'
};

-- Daily citation counts -> RRD consolidation into a weekly velocity rollup.
-- The rollup materializes asynchronously within the window; the trend signal
-- used for reranking is read straight from this (always-populated) source table.
CREATE TABLE IF NOT EXISTS paper_citations_weekly (
    paper_id       int,
    day            timestamp,
    citations_sum  int,
    citations_max  int,
    citations_avg  double,
    PRIMARY KEY (paper_id, day)
) WITH CLUSTERING ORDER BY (day DESC);

CREATE TABLE IF NOT EXISTS paper_citations_daily (
    paper_id  int,
    day       timestamp,
    citations int,
    PRIMARY KEY (paper_id, day)
) WITH CLUSTERING ORDER BY (day DESC)
  AND extensions = {
    'consolidation.interval': '168h',
    'consolidation.functions': 'sum,max,avg',
    'consolidation.target': 'paper_citations_weekly',
    'consolidation.columns': 'citations'
  };
