CREATE KEYSPACE IF NOT EXISTS social WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE social;

-- Person vertex table
CREATE TABLE person (
    person_id uuid PRIMARY KEY,
    name text,
    age int,
    city text,
    joined_at timestamp
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Person'
};

-- Company vertex table
CREATE TABLE company (
    company_id uuid PRIMARY KEY,
    name text,
    industry text,
    city text,
    founded_year int
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Company'
};

-- Post vertex table
CREATE TABLE post (
    post_id uuid PRIMARY KEY,
    author_id uuid,
    content text,
    created_at timestamp,
    tags set<text>
) WITH extensions = {
    'graph.type': 'vertex',
    'graph.label': 'Post'
};

-- Person follows Person
CREATE TABLE follows (
    source_id uuid,
    target_id uuid,
    since timestamp,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'FOLLOWS',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Person',
    'graph.target_label': 'Person'
};

-- Person likes Post
CREATE TABLE likes (
    source_id uuid,
    target_id uuid,
    liked_at timestamp,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'LIKES',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Person',
    'graph.target_label': 'Post'
};

-- Person works at Company
CREATE TABLE works_at (
    source_id uuid,
    target_id uuid,
    role text,
    started_at timestamp,
    PRIMARY KEY (source_id, target_id)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'WORKS_AT',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Person',
    'graph.target_label': 'Company'
};

-- Person transfers money to Person (for fraud detection)
CREATE TABLE transfers (
    source_id uuid,
    target_id uuid,
    amount decimal,
    transferred_at timestamp,
    reference text,
    PRIMARY KEY (source_id, target_id, transferred_at)
) WITH extensions = {
    'graph.type': 'edge',
    'graph.label': 'TRANSFERS',
    'graph.source': 'source_id',
    'graph.target': 'target_id',
    'graph.source_label': 'Person',
    'graph.target_label': 'Person'
};

-- Secondary indexes for reverse lookups
CREATE INDEX ON person (city);
CREATE INDEX ON person (name);
CREATE INDEX ON company (industry);
CREATE INDEX ON post (author_id);
