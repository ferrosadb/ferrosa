CREATE KEYSPACE IF NOT EXISTS personalization
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  };

USE personalization;

CREATE TABLE users (
    user_id             uuid PRIMARY KEY,
    name                text,
    email               text,
    signup_date         date,
    preferred_genres    set<text>,
    preferred_language  text
);

CREATE TABLE content (
    content_id    uuid PRIMARY KEY,
    title         text,
    content_type  text,       -- movie, series, documentary
    genre         text,
    release_year  int,
    rating_avg    float,
    tags          set<text>
);

CREATE TABLE watch_history (
    user_id       uuid,
    watched_at    timestamp,
    content_id    uuid,
    title         text,
    progress_pct  int,
    rating        int,
    PRIMARY KEY (user_id, watched_at)
) WITH CLUSTERING ORDER BY (watched_at DESC);

CREATE TABLE recommendations (
    user_id     uuid,
    rank        int,
    content_id  uuid,
    title       text,
    score       float,
    reason      text,
    PRIMARY KEY (user_id, rank)
);

CREATE INDEX idx_content_genre ON content (genre);
