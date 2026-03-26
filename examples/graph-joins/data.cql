USE social;

-- People
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, 'Alice',   32, 'San Francisco', '2024-01-15T08:00:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, 'Bob',     28, 'San Francisco', '2024-02-20T10:30:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, 'Carol',   35, 'New York',      '2024-03-10T14:00:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (dddddddd-dddd-dddd-dddd-dddddddddddd, 'David',   41, 'New York',      '2024-03-22T09:15:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 'Eve',     29, 'Austin',        '2024-04-05T16:45:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (ffffffff-ffff-ffff-ffff-ffffffffffff, 'Frank',   38, 'Austin',        '2024-05-12T11:00:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (11111111-1111-1111-1111-111111111111, 'Grace',   26, 'San Francisco', '2024-06-18T07:30:00Z');
INSERT INTO person (person_id, name, age, city, joined_at) VALUES
  (22222222-2222-2222-2222-222222222222, 'Hank',    45, 'Chicago',       '2024-07-01T13:20:00Z');

-- Companies
INSERT INTO company (company_id, name, industry, city, founded_year) VALUES
  (c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0, 'Acme Corp',    'Technology', 'San Francisco', 2015);
INSERT INTO company (company_id, name, industry, city, founded_year) VALUES
  (d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0, 'Globex Inc',   'Finance',    'New York',      2008);
INSERT INTO company (company_id, name, industry, city, founded_year) VALUES
  (e0e0e0e0-e0e0-e0e0-e0e0-e0e0e0e0e0e0, 'StartupXYZ',   'Technology', 'Austin',        2022);

-- Posts
INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000001-0000-0000-0000-000000000001,
   aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
   'Just deployed our new graph engine. The future is traversals!',
   '2025-01-10T09:00:00Z', {'ferrosa', 'graph'});

INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000002-0000-0000-0000-000000000002,
   bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb,
   'Hot take: denormalization is a workaround, not a strategy.',
   '2025-01-12T14:30:00Z', {'databases', 'opinion'});

INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000003-0000-0000-0000-000000000003,
   cccccccc-cccc-cccc-cccc-cccccccccccc,
   'Wrote a fraud detection pipeline in 20 lines of Cypher. Mind blown.',
   '2025-01-15T11:00:00Z', {'fraud', 'cypher'});

INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000004-0000-0000-0000-000000000004,
   eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee,
   'Exploring lock-free data structures for concurrent graph indexing.',
   '2025-01-18T16:00:00Z', {'concurrency', 'research'});

INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000005-0000-0000-0000-000000000005,
   dddddddd-dddd-dddd-dddd-dddddddddddd,
   'Benchmarked Ferrosa against Cassandra + Neo4j. Single system wins.',
   '2025-01-20T08:45:00Z', {'benchmark', 'ferrosa'});

INSERT INTO post (post_id, author_id, content, created_at, tags) VALUES
  (b0000006-0000-0000-0000-000000000006,
   11111111-1111-1111-1111-111111111111,
   'Graph indexes + S3 write-behind = best of all worlds.',
   '2025-01-22T10:30:00Z', {'ferrosa', 's3'});

-- Follow relationships
-- Alice's network: follows Bob, Carol, and Eve
INSERT INTO follows (source_id, target_id, since) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, '2024-02-21T00:00:00Z');
INSERT INTO follows (source_id, target_id, since) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, cccccccc-cccc-cccc-cccc-cccccccccccc, '2024-03-11T00:00:00Z');
INSERT INTO follows (source_id, target_id, since) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, '2024-04-10T00:00:00Z');

-- Bob follows Carol and David
INSERT INTO follows (source_id, target_id, since) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, cccccccc-cccc-cccc-cccc-cccccccccccc, '2024-03-15T00:00:00Z');
INSERT INTO follows (source_id, target_id, since) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, dddddddd-dddd-dddd-dddd-dddddddddddd, '2024-04-01T00:00:00Z');

-- Carol follows Alice and Frank (creates a cycle: Alice -> Carol -> Alice)
INSERT INTO follows (source_id, target_id, since) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2024-03-12T00:00:00Z');
INSERT INTO follows (source_id, target_id, since) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, ffffffff-ffff-ffff-ffff-ffffffffffff, '2024-06-01T00:00:00Z');

-- Eve follows Frank and Grace
INSERT INTO follows (source_id, target_id, since) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, ffffffff-ffff-ffff-ffff-ffffffffffff, '2024-05-15T00:00:00Z');
INSERT INTO follows (source_id, target_id, since) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 11111111-1111-1111-1111-111111111111, '2024-06-20T00:00:00Z');

-- David follows Hank
INSERT INTO follows (source_id, target_id, since) VALUES
  (dddddddd-dddd-dddd-dddd-dddddddddddd, 22222222-2222-2222-2222-222222222222, '2024-07-05T00:00:00Z');

-- Grace follows Alice
INSERT INTO follows (source_id, target_id, since) VALUES
  (11111111-1111-1111-1111-111111111111, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, '2024-06-19T00:00:00Z');

-- Likes
-- Alice likes Bob's post and Carol's post
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, b0000002-0000-0000-0000-000000000002, '2025-01-12T15:00:00Z');
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, b0000003-0000-0000-0000-000000000003, '2025-01-15T12:00:00Z');

-- Bob likes Alice's post and Eve's post
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, b0000001-0000-0000-0000-000000000001, '2025-01-10T10:00:00Z');
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, b0000004-0000-0000-0000-000000000004, '2025-01-19T08:00:00Z');

-- Carol likes Alice's post and David's post
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, b0000001-0000-0000-0000-000000000001, '2025-01-11T09:00:00Z');
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, b0000005-0000-0000-0000-000000000005, '2025-01-20T10:00:00Z');

-- Eve likes Bob's post and Carol's post
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, b0000002-0000-0000-0000-000000000002, '2025-01-13T07:30:00Z');
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, b0000003-0000-0000-0000-000000000003, '2025-01-16T11:00:00Z');

-- David likes Grace's post
INSERT INTO likes (source_id, target_id, liked_at) VALUES
  (dddddddd-dddd-dddd-dddd-dddddddddddd, b0000006-0000-0000-0000-000000000006, '2025-01-22T14:00:00Z');

-- Employment
-- Alice and Bob work at Acme Corp
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0,
   'Staff Engineer', '2020-06-01T00:00:00Z');
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0,
   'Senior Engineer', '2022-01-15T00:00:00Z');

-- Carol and David work at Globex Inc
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (cccccccc-cccc-cccc-cccc-cccccccccccc, d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0,
   'VP of Engineering', '2018-03-01T00:00:00Z');
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (dddddddd-dddd-dddd-dddd-dddddddddddd, d0d0d0d0-d0d0-d0d0-d0d0-d0d0d0d0d0d0,
   'Data Scientist', '2021-09-01T00:00:00Z');

-- Eve and Frank work at StartupXYZ
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, e0e0e0e0-e0e0-e0e0-e0e0-e0e0e0e0e0e0,
   'Co-Founder', '2022-01-01T00:00:00Z');
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (ffffffff-ffff-ffff-ffff-ffffffffffff, e0e0e0e0-e0e0-e0e0-e0e0-e0e0e0e0e0e0,
   'CTO', '2022-01-01T00:00:00Z');

-- Grace works at Acme Corp
INSERT INTO works_at (source_id, target_id, role, started_at) VALUES
  (11111111-1111-1111-1111-111111111111, c0c0c0c0-c0c0-c0c0-c0c0-c0c0c0c0c0c0,
   'Junior Engineer', '2024-06-20T00:00:00Z');

-- Transfers (for fraud ring detection)
-- Alice -> David: $5,000
INSERT INTO transfers (source_id, target_id, amount, transferred_at, reference) VALUES
  (aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa, dddddddd-dddd-dddd-dddd-dddddddddddd,
   5000.00, '2025-02-01T10:00:00Z', 'consulting');

-- David -> Frank: $4,800
INSERT INTO transfers (source_id, target_id, amount, transferred_at, reference) VALUES
  (dddddddd-dddd-dddd-dddd-dddddddddddd, ffffffff-ffff-ffff-ffff-ffffffffffff,
   4800.00, '2025-02-02T14:30:00Z', 'equipment');

-- Frank -> Alice: $4,500 (completes the ring)
INSERT INTO transfers (source_id, target_id, amount, transferred_at, reference) VALUES
  (ffffffff-ffff-ffff-ffff-ffffffffffff, aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
   4500.00, '2025-02-03T09:15:00Z', 'refund');

-- Legitimate (non-circular) transfers
INSERT INTO transfers (source_id, target_id, amount, transferred_at, reference) VALUES
  (bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb, cccccccc-cccc-cccc-cccc-cccccccccccc,
   250.00, '2025-02-01T11:00:00Z', 'lunch');
INSERT INTO transfers (source_id, target_id, amount, transferred_at, reference) VALUES
  (eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee, 11111111-1111-1111-1111-111111111111,
   75.00, '2025-02-04T16:00:00Z', 'book');
