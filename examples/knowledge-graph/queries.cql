USE knowledge;

-- ============================================================
-- Basic lookups by primary key
-- ============================================================

-- Full row for a single researcher
SELECT * FROM researcher WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Project specific columns
SELECT name, h_index, specializations FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Look up a paper by ID
SELECT title, venue, year, citation_count FROM paper
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Look up a grant
SELECT title, funder, amount, currency FROM grant_award
WHERE grant_id = 55555555-0000-0000-0000-000000000001;

-- ============================================================
-- Secondary index queries
-- ============================================================

-- Papers at a specific venue
SELECT title, year, citation_count FROM paper WHERE venue = 'NeurIPS';

-- Papers from a specific year
SELECT title, venue, citation_count FROM paper WHERE year = 2024;

-- Institutions in a country
SELECT name, city, ranking FROM institution WHERE country = 'United States';

-- Find a topic by name
SELECT * FROM topic WHERE name = 'Machine Learning';

-- Active grants from a specific funder
SELECT title, amount, currency, start_date, end_date FROM grant_award
WHERE funder = 'NSF';

-- Active grants by status
SELECT title, funder, amount FROM grant_award WHERE status = 'active';

-- Papers by DOI
SELECT title, venue, year FROM paper WHERE doi = '10.1234/neurips.2024.001';

-- ============================================================
-- Collection operations
-- ============================================================

-- Read a set column
SELECT specializations FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Read a map column
SELECT social_links FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Read keywords set from a paper
SELECT keywords FROM paper
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Read URLs map from a paper
SELECT urls FROM paper
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Read departments from institution
SELECT departments FROM institution
WHERE institution_id = 22222222-0000-0000-0000-000000000001;

-- ============================================================
-- Count and aggregation
-- ============================================================

SELECT COUNT(*) FROM paper;
SELECT COUNT(*) FROM researcher;
SELECT COUNT(*) FROM institution;
SELECT COUNT(*) FROM authored;
SELECT COUNT(*) FROM cites;

-- ============================================================
-- LIMIT
-- ============================================================

SELECT title, venue, year FROM paper LIMIT 5;
SELECT name, h_index FROM researcher LIMIT 3;

-- ============================================================
-- Edge table queries (clustering key traversals)
-- ============================================================

-- All papers authored by Alice
SELECT target_id, author_position, corresponding FROM authored
WHERE source_id = 11111111-0000-0000-0000-000000000001;

-- All citations from Paper 1 (Sparse Transformers)
SELECT target_id, context, section FROM cites
WHERE source_id = 33333333-0000-0000-0000-000000000001;

-- All citations from Paper 3 (Medical ViTs) -- multiple citations
SELECT target_id, context, section FROM cites
WHERE source_id = 33333333-0000-0000-0000-000000000003;

-- All topics covered by Paper 4 (Federated Privacy)
SELECT target_id, relevance FROM covers_topic
WHERE source_id = 33333333-0000-0000-0000-000000000004;

-- All papers funded by the DARPA grant
SELECT source_id, role FROM funded_by
WHERE target_id = 55555555-0000-0000-0000-000000000002 ALLOW FILTERING;

-- Alice's collaborators
SELECT target_id, paper_count, first_collab_year, last_collab_year FROM collaborates_with
WHERE source_id = 11111111-0000-0000-0000-000000000001;

-- ============================================================
-- Multiple WHERE conditions (partition + clustering)
-- ============================================================

-- Specific affiliation: Alice at MIT
SELECT department, role, start_year, end_year FROM affiliated_with
WHERE source_id = 11111111-0000-0000-0000-000000000001
  AND target_id = 22222222-0000-0000-0000-000000000001;

-- Specific authorship: Alice authored Paper 1
SELECT author_position, corresponding FROM authored
WHERE source_id = 11111111-0000-0000-0000-000000000001
  AND target_id = 33333333-0000-0000-0000-000000000001;

-- Specific citation: Paper 12 cites Paper 2
SELECT context, section FROM cites
WHERE source_id = 33333333-0000-0000-0000-000000000012
  AND target_id = 33333333-0000-0000-0000-000000000002;

-- ============================================================
-- UPDATE: collection manipulation
-- ============================================================

-- Add a specialization to a researcher's set
UPDATE researcher SET specializations = specializations + {'Reinforcement Learning'}
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Add a social link to a researcher's map
UPDATE researcher SET social_links['linkedin'] = 'https://linkedin.com/in/alicechen'
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Add a keyword to a paper
UPDATE paper SET keywords = keywords + {'efficiency'}
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Update a URL in a paper's map
UPDATE paper SET urls['slides'] = 'https://neurips.cc/slides/sparse-tf'
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Update a scalar column
UPDATE paper SET citation_count = 156
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Update h_index
UPDATE researcher SET h_index = 47
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Add a department to an institution
UPDATE institution SET departments = departments + {'Linguistics'}
WHERE institution_id = 22222222-0000-0000-0000-000000000001;

-- Verify the updates
SELECT specializations, social_links FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;
SELECT citation_count, keywords, urls FROM paper
WHERE paper_id = 33333333-0000-0000-0000-000000000001;
SELECT departments FROM institution
WHERE institution_id = 22222222-0000-0000-0000-000000000001;

-- ============================================================
-- DELETE: collection element removal
-- ============================================================

-- Remove a specialization from a set
UPDATE researcher SET specializations = specializations - {'Reinforcement Learning'}
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Remove a social link from a map
DELETE social_links['linkedin'] FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- Remove a keyword from a paper
UPDATE paper SET keywords = keywords - {'efficiency'}
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Remove a URL from a paper's map
DELETE urls['slides'] FROM paper
WHERE paper_id = 33333333-0000-0000-0000-000000000001;

-- Remove a department from an institution
UPDATE institution SET departments = departments - {'Linguistics'}
WHERE institution_id = 22222222-0000-0000-0000-000000000001;

-- ============================================================
-- INSERT IF NOT EXISTS (lightweight transactions)
-- ============================================================

-- Conditionally insert a new researcher (only if UUID doesn't exist)
INSERT INTO researcher (researcher_id, name, email, active)
VALUES (11111111-0000-0000-0000-000000000099, 'New Researcher', 'new@test.com', true)
IF NOT EXISTS;

-- Conditionally insert a duplicate (should return applied=false)
INSERT INTO researcher (researcher_id, name, email, active)
VALUES (11111111-0000-0000-0000-000000000001, 'Duplicate Alice', 'dup@test.com', true)
IF NOT EXISTS;

-- Conditionally insert a new topic
INSERT INTO topic (topic_id, name, description, paper_count)
VALUES (44444444-0000-0000-0000-000000000099, 'Quantum Computing',
        'Computation using quantum mechanical phenomena', 3200)
IF NOT EXISTS;

-- ============================================================
-- USING TTL (time-to-live for expiring data)
-- ============================================================

-- Insert a preprint that expires after 24 hours
INSERT INTO paper (paper_id, title, year, paper_type)
VALUES (33333333-0000-0000-0000-000000000099, 'Preprint Draft: Preliminary Results', 2024, 'Preprint')
USING TTL 86400;

-- Insert a temporary collaboration record (30-day TTL)
INSERT INTO collaborates_with (source_id, target_id, paper_count, first_collab_year, last_collab_year)
VALUES (11111111-0000-0000-0000-000000000099, 11111111-0000-0000-0000-000000000001, 0, 2024, 2024)
USING TTL 2592000;

-- ============================================================
-- USING TIMESTAMP (explicit write timestamps)
-- ============================================================

-- Insert a topic with an explicit microsecond timestamp
INSERT INTO topic (topic_id, name, description, paper_count)
VALUES (44444444-0000-0000-0000-000000000098, 'Quantum Computing',
        'Computation using quantum mechanical phenomena', 3200)
USING TIMESTAMP 1704067200000000;

-- Update with an explicit timestamp
UPDATE researcher USING TIMESTAMP 1704067200000000
SET citations_total = 13000
WHERE researcher_id = 11111111-0000-0000-0000-000000000001;

-- ============================================================
-- DELETE operations
-- ============================================================

-- Delete a specific row
DELETE FROM collaborates_with
WHERE source_id = 11111111-0000-0000-0000-000000000099
  AND target_id = 11111111-0000-0000-0000-000000000001;

-- Delete a specific column value
DELETE email FROM researcher
WHERE researcher_id = 11111111-0000-0000-0000-000000000099;

-- Clean up test data
DELETE FROM researcher WHERE researcher_id = 11111111-0000-0000-0000-000000000099;
DELETE FROM paper WHERE paper_id = 33333333-0000-0000-0000-000000000099;
DELETE FROM topic WHERE topic_id = 44444444-0000-0000-0000-000000000099;
DELETE FROM topic WHERE topic_id = 44444444-0000-0000-0000-000000000098;

-- ============================================================
-- ALLOW FILTERING (full-table scans with non-indexed columns)
-- ============================================================

-- Find researchers by title (no index on title)
SELECT name, title, h_index FROM researcher WHERE title = 'Professor' ALLOW FILTERING;

-- Find papers of a specific type
SELECT title, venue, year FROM paper WHERE paper_type = 'Journal' ALLOW FILTERING;

-- Find corresponding authors
SELECT source_id, target_id FROM authored WHERE corresponding = true ALLOW FILTERING;

-- Find high-relevance topic coverage
SELECT source_id, target_id, relevance FROM covers_topic WHERE relevance > 0.9 ALLOW FILTERING;
