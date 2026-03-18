USE social;

-- Find people in a city (secondary index scan)
SELECT name, age FROM person WHERE city = 'San Francisco';

-- Find who Alice follows (partition key lookup)
SELECT target_id, since FROM follows
WHERE source_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- Find posts by a specific author (secondary index on author_id)
SELECT content, created_at FROM post
WHERE author_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- Find technology companies (secondary index on industry)
SELECT name, city, founded_year FROM company
WHERE industry = 'Technology';

-- Find who follows Alice (reverse lookup)
SELECT source_id, since FROM follows
WHERE target_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
ALLOW FILTERING;
