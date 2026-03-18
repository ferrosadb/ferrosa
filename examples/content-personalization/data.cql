USE personalization;

-- User profiles
INSERT INTO users (user_id, name, email, signup_date, preferred_genres, preferred_language)
VALUES (aaaa1111-1111-1111-1111-111111111111,
        'Lena Johansson', 'lena@example.com', '2025-08-10',
        {'sci-fi', 'thriller', 'documentary'}, 'en');

INSERT INTO users (user_id, name, email, signup_date, preferred_genres, preferred_language)
VALUES (bbbb2222-2222-2222-2222-222222222222,
        'Marco Rossi', 'marco@example.com', '2025-11-22',
        {'comedy', 'drama', 'romance'}, 'it');

INSERT INTO users (user_id, name, email, signup_date, preferred_genres, preferred_language)
VALUES (cccc3333-3333-3333-3333-333333333333,
        'Yuki Tanaka', 'yuki@example.com', '2026-01-05',
        {'anime', 'sci-fi', 'action'}, 'ja');

-- Content catalog
INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Stellar Drift', 'movie', 'sci-fi', 2025, 4.5, {'space', 'exploration', 'award-winner'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'The Last Algorithm', 'series', 'thriller', 2026, 4.7, {'tech', 'suspense', 'binge-worthy'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Ocean Depths', 'documentary', 'documentary', 2025, 4.8, {'nature', 'marine-biology', 'stunning-visuals'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Roman Holiday Redux', 'movie', 'romance', 2026, 3.9, {'italy', 'travel', 'feel-good'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Laughing Matters', 'series', 'comedy', 2025, 4.2, {'standup', 'improv', 'light-hearted'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Neon Ronin', 'series', 'anime', 2026, 4.6, {'cyberpunk', 'action', 'japanese'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Quantum Entangled', 'movie', 'sci-fi', 2024, 4.1, {'physics', 'mind-bending', 'indie'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'The Silk Road Diaries', 'documentary', 'documentary', 2025, 4.4, {'history', 'travel', 'cultural'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Midnight in Milan', 'movie', 'drama', 2026, 4.3, {'italy', 'family', 'emotional'});

INSERT INTO content (content_id, title, content_type, genre, release_year, rating_avg, tags)
VALUES (uuid(), 'Blade Circuit', 'movie', 'action', 2025, 3.8, {'martial-arts', 'futuristic', 'fast-paced'});

-- Watch history: Lena Johansson (sci-fi and documentary fan)
INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (aaaa1111-1111-1111-1111-111111111111, '2026-03-10 20:00:00', uuid(), 'Stellar Drift', 100, 5);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (aaaa1111-1111-1111-1111-111111111111, '2026-03-12 19:30:00', uuid(), 'The Last Algorithm', 60, 0);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (aaaa1111-1111-1111-1111-111111111111, '2026-03-14 21:00:00', uuid(), 'Ocean Depths', 100, 5);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (aaaa1111-1111-1111-1111-111111111111, '2026-03-16 20:15:00', uuid(), 'Quantum Entangled', 100, 4);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (aaaa1111-1111-1111-1111-111111111111, '2026-03-17 22:00:00', uuid(), 'The Silk Road Diaries', 35, 0);

-- Watch history: Marco Rossi (comedy and drama fan)
INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (bbbb2222-2222-2222-2222-222222222222, '2026-03-11 18:00:00', uuid(), 'Laughing Matters', 100, 4);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (bbbb2222-2222-2222-2222-222222222222, '2026-03-13 20:30:00', uuid(), 'Midnight in Milan', 100, 5);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (bbbb2222-2222-2222-2222-222222222222, '2026-03-15 19:00:00', uuid(), 'Roman Holiday Redux', 100, 3);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (bbbb2222-2222-2222-2222-222222222222, '2026-03-16 21:00:00', uuid(), 'Ocean Depths', 45, 0);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (bbbb2222-2222-2222-2222-222222222222, '2026-03-17 20:00:00', uuid(), 'The Last Algorithm', 25, 0);

-- Watch history: Yuki Tanaka (anime and action fan)
INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (cccc3333-3333-3333-3333-333333333333, '2026-03-09 22:00:00', uuid(), 'Neon Ronin', 100, 5);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (cccc3333-3333-3333-3333-333333333333, '2026-03-11 21:30:00', uuid(), 'Blade Circuit', 100, 4);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (cccc3333-3333-3333-3333-333333333333, '2026-03-14 20:00:00', uuid(), 'Stellar Drift', 100, 4);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (cccc3333-3333-3333-3333-333333333333, '2026-03-16 19:00:00', uuid(), 'Quantum Entangled', 80, 0);

INSERT INTO watch_history (user_id, watched_at, content_id, title, progress_pct, rating)
VALUES (cccc3333-3333-3333-3333-333333333333, '2026-03-17 21:00:00', uuid(), 'The Last Algorithm', 100, 5);

-- Recommendations for Lena (sci-fi, documentary fan)
INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (aaaa1111-1111-1111-1111-111111111111, 1, uuid(), 'Neon Ronin', 0.94, 'Fans of Stellar Drift also loved this cyberpunk epic');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (aaaa1111-1111-1111-1111-111111111111, 2, uuid(), 'Blade Circuit', 0.87, 'Action sci-fi with themes you enjoy');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (aaaa1111-1111-1111-1111-111111111111, 3, uuid(), 'Midnight in Milan', 0.72, 'Highly rated drama trending this week');

-- Recommendations for Marco (comedy, drama fan)
INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (bbbb2222-2222-2222-2222-222222222222, 1, uuid(), 'The Silk Road Diaries', 0.91, 'You loved Italian settings — explore more cultural stories');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (bbbb2222-2222-2222-2222-222222222222, 2, uuid(), 'Stellar Drift', 0.83, 'Award-winning film popular with drama fans');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (bbbb2222-2222-2222-2222-222222222222, 3, uuid(), 'Neon Ronin', 0.68, 'Trending series with strong character development');

-- Recommendations for Yuki (anime, action fan)
INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (cccc3333-3333-3333-3333-333333333333, 1, uuid(), 'Ocean Depths', 0.89, 'Visually stunning — similar to anime art you enjoy');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (cccc3333-3333-3333-3333-333333333333, 2, uuid(), 'Laughing Matters', 0.76, 'Light palate cleanser between action binges');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (cccc3333-3333-3333-3333-333333333333, 3, uuid(), 'Roman Holiday Redux', 0.61, 'Popular with users who have similar taste profiles');

INSERT INTO recommendations (user_id, rank, content_id, title, score, reason)
VALUES (cccc3333-3333-3333-3333-333333333333, 4, uuid(), 'Midnight in Milan', 0.55, 'Emotional storytelling with cinematic visuals');
