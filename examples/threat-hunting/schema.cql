-- Threat hunting, multi-model: one corpus of indicators (IOCs), queried five ways.
--   * keyword (LIKE)      on description    -> WHERE description LIKE '%...%'
--   * vector ANN (HVQ)    on embedding       -> USING 'vector'    + ORDER BY ... ANN OF
--   * phonetic (Metaphone) on actor name      -> USING 'phonetic'  + SOUNDS LIKE
--   * RRD consolidation   on activity counts  -> consolidation.*    (spike signal)
--   * property graph      on the same tables  -> graph.*            (Cypher infrastructure)
-- See threat-hunting.adoc for the reciprocal-rank-fusion hunt.

CREATE KEYSPACE IF NOT EXISTS threat
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE threat;

-- Indicators of compromise: full-text searchable description, a real 768-d
-- behaviour embedding (nomic-embed-text-v2-moe), and a graph vertex.
CREATE TABLE IF NOT EXISTS indicator (
    indicator_id int PRIMARY KEY,
    value        text,
    kind         text,
    description  text,
    embedding    vector<float, 768>
) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Indicator' };

CREATE INDEX IF NOT EXISTS indicator_ann ON indicator (embedding) USING 'vector'
    WITH OPTIONS = {'method': 'hvq'};

-- Threat actors / malware families: phonetic name search ("Lazerus" -> "Lazarus").
CREATE TABLE IF NOT EXISTS actor (
    actor_id int PRIMARY KEY,
    name     text,
    origin   text
) WITH extensions = { 'graph.type': 'vertex', 'graph.label': 'Actor' };

CREATE INDEX IF NOT EXISTS actor_phonetic ON actor (name) USING 'phonetic';

-- Attribution edges (Indicator)-[:ATTRIBUTED_TO]->(Actor).
CREATE TABLE IF NOT EXISTS attributed_to (
    indicator_id int,
    actor_id     int,
    PRIMARY KEY (indicator_id, actor_id)
) WITH extensions = {
    'graph.type': 'edge', 'graph.label': 'ATTRIBUTED_TO',
    'graph.source': 'indicator_id', 'graph.target': 'actor_id',
    'graph.source_label': 'Indicator', 'graph.target_label': 'Actor'
};

-- Infrastructure edges (Indicator)-[:COMMUNICATES_WITH]->(Indicator).
CREATE TABLE IF NOT EXISTS communicates_with (
    src_indicator_id int,
    dst_indicator_id int,
    PRIMARY KEY (src_indicator_id, dst_indicator_id)
) WITH extensions = {
    'graph.type': 'edge', 'graph.label': 'COMMUNICATES_WITH',
    'graph.source': 'src_indicator_id', 'graph.target': 'dst_indicator_id',
    'graph.source_label': 'Indicator', 'graph.target_label': 'Indicator'
};

-- Per-minute activity hits -> RRD consolidation into an hourly spike rollup.
-- The rollup materializes asynchronously; the hunt reads the (always-populated)
-- source table for recent velocity.
CREATE TABLE IF NOT EXISTS indicator_activity_hourly (
    indicator_id int,
    minute       timestamp,
    hits_sum     int,
    hits_max     int,
    PRIMARY KEY (indicator_id, minute)
) WITH CLUSTERING ORDER BY (minute DESC);

CREATE TABLE IF NOT EXISTS indicator_activity (
    indicator_id int,
    minute       timestamp,
    hits         int,
    PRIMARY KEY (indicator_id, minute)
) WITH CLUSTERING ORDER BY (minute DESC)
  AND extensions = {
    'consolidation.interval': '1h',
    'consolidation.functions': 'sum,max',
    'consolidation.target': 'indicator_activity_hourly',
    'consolidation.columns': 'hits'
  };
