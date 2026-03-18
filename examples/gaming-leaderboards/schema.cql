CREATE KEYSPACE IF NOT EXISTS gaming
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
  };

USE gaming;

CREATE TABLE players (
    player_id         uuid PRIMARY KEY,
    username          text,
    display_name      text,
    level             int,
    xp                bigint,
    wins              int,
    losses            int,
    kill_death_ratio  float,
    created_at        timestamp,
    last_login        timestamp
);

CREATE TABLE matches (
    game_mode         text,
    started_at        timestamp,
    match_id          uuid,
    map_name          text,
    duration_seconds  int,
    winner_id         uuid,
    players           list<text>,
    PRIMARY KEY (game_mode, started_at, match_id)
) WITH CLUSTERING ORDER BY (started_at DESC, match_id ASC);

CREATE TABLE player_matches (
    player_id    uuid,
    match_time   timestamp,
    match_id     uuid,
    kills        int,
    deaths       int,
    assists      int,
    score        int,
    result       text,       -- win, loss, draw
    PRIMARY KEY (player_id, match_time, match_id)
) WITH CLUSTERING ORDER BY (match_time DESC, match_id ASC);

CREATE TABLE leaderboard (
    game_mode   text,
    rank        int,
    player_id   uuid,
    username    text,
    score       bigint,
    PRIMARY KEY (game_mode, rank)
);
