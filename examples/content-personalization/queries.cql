USE personalization;

-- Lena's recent viewing activity
SELECT watched_at, title, progress_pct, rating
FROM watch_history
WHERE user_id = aaaa1111-1111-1111-1111-111111111111
LIMIT 10;

-- Yuki's personalized recommendations
SELECT rank, title, score, reason
FROM recommendations
WHERE user_id = cccc3333-3333-3333-3333-333333333333;

-- Browse all sci-fi content
SELECT title, content_type, release_year, rating_avg
FROM content
WHERE genre = 'sci-fi';

-- All content rated 4.5 or higher
SELECT title, content_type, genre, rating_avg
FROM content
WHERE rating_avg >= 4.5
ALLOW FILTERING;

-- Marco paused Ocean Depths at 45%, now resuming to 78%
UPDATE watch_history
SET progress_pct = 78
WHERE user_id = bbbb2222-2222-2222-2222-222222222222
  AND watched_at = '2026-03-16 21:00:00';

-- Find users who like sci-fi
SELECT user_id, name, preferred_genres
FROM users
WHERE preferred_genres CONTAINS 'sci-fi'
ALLOW FILTERING;
