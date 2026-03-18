CREATE KEYSPACE IF NOT EXISTS knowledge WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE knowledge;

-- ============================================================
-- Vertex tables (entities)
-- ============================================================

-- Researcher: an academic with publications, metrics, and social links
CREATE TABLE IF NOT EXISTS researcher (
    researcher_id uuid PRIMARY KEY,
    name text,
    email text,
    title text,
    h_index int,
    citations_total int,
    orcid text,
    joined_at timestamp,
    active boolean,
    specializations set<text>,
    social_links map<text, text>
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Researcher'
};

-- Institution: a university or research lab
CREATE TABLE IF NOT EXISTS institution (
    institution_id uuid PRIMARY KEY,
    name text,
    country text,
    city text,
    institution_type text,
    ranking int,
    founded_year int,
    departments set<text>,
    metadata map<text, text>
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Institution'
};

-- Paper: a published research paper
CREATE TABLE IF NOT EXISTS paper (
    paper_id uuid PRIMARY KEY,
    title text,
    abstract_text text,
    doi text,
    venue text,
    year int,
    month int,
    citation_count int,
    paper_type text,
    keywords set<text>,
    urls map<text, text>
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Paper'
};

-- Topic: a research area or subfield
CREATE TABLE IF NOT EXISTS topic (
    topic_id uuid PRIMARY KEY,
    name text,
    parent_topic text,
    description text,
    paper_count int
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Topic'
};

-- Grant: a funding award from a government or private funder
CREATE TABLE IF NOT EXISTS grant_award (
    grant_id uuid PRIMARY KEY,
    title text,
    funder text,
    amount decimal,
    currency text,
    start_date date,
    end_date date,
    status text
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Grant'
};

-- ============================================================
-- Edge tables (relationships)
-- ============================================================

-- Researcher AUTHORED Paper
CREATE TABLE IF NOT EXISTS authored (
    source_id uuid,
    target_id uuid,
    author_position int,
    corresponding boolean,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'AUTHORED',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Researcher',
    'graph.target_label': 'Paper'
};

-- Paper CITES Paper
CREATE TABLE IF NOT EXISTS cites (
    source_id uuid,
    target_id uuid,
    context text,
    section text,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'CITES',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Paper',
    'graph.target_label': 'Paper'
};

-- Researcher AFFILIATED_WITH Institution
CREATE TABLE IF NOT EXISTS affiliated_with (
    source_id uuid,
    target_id uuid,
    department text,
    role text,
    start_year int,
    end_year int,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'AFFILIATED_WITH',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Researcher',
    'graph.target_label': 'Institution'
};

-- Paper COVERS Topic
CREATE TABLE IF NOT EXISTS covers_topic (
    source_id uuid,
    target_id uuid,
    relevance float,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'COVERS',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Paper',
    'graph.target_label': 'Topic'
};

-- Paper FUNDED_BY Grant
CREATE TABLE IF NOT EXISTS funded_by (
    source_id uuid,
    target_id uuid,
    role text,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'FUNDED_BY',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Paper',
    'graph.target_label': 'Grant'
};

-- Researcher COLLABORATES with Researcher
CREATE TABLE IF NOT EXISTS collaborates_with (
    source_id uuid,
    target_id uuid,
    paper_count int,
    first_collab_year int,
    last_collab_year int,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'COLLABORATES',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Researcher',
    'graph.target_label': 'Researcher'
};

-- ============================================================
-- Secondary indexes for CQL queries
-- ============================================================

CREATE INDEX IF NOT EXISTS ON researcher (name);
CREATE INDEX IF NOT EXISTS ON researcher (active);
CREATE INDEX IF NOT EXISTS ON paper (venue);
CREATE INDEX IF NOT EXISTS ON paper (year);
CREATE INDEX IF NOT EXISTS ON paper (doi);
CREATE INDEX IF NOT EXISTS ON institution (country);
CREATE INDEX IF NOT EXISTS ON institution (name);
CREATE INDEX IF NOT EXISTS ON topic (name);
CREATE INDEX IF NOT EXISTS ON grant_award (funder);
CREATE INDEX IF NOT EXISTS ON grant_award (status);
