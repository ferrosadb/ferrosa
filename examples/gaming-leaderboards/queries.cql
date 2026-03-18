USE gaming;

-- Look up PixelQueen's profile
SELECT display_name, level, xp, wins, losses, kill_death_ratio, last_login
FROM players
WHERE player_id = c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3;

-- ShadowFox's last 5 matches
SELECT match_time, kills, deaths, assists, score, result
FROM player_matches
WHERE player_id = a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1
LIMIT 5;

-- Top 10 players in ranked mode
SELECT rank, username, score
FROM leaderboard
WHERE game_mode = 'ranked'
LIMIT 10;

-- Last 5 ranked matches
SELECT started_at, map_name, duration_seconds, players
FROM matches
WHERE game_mode = 'ranked'
LIMIT 5;

-- StarBreaker's win/loss record
SELECT display_name, wins, losses, kill_death_ratio
FROM players
WHERE player_id = e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5;

-- PixelQueen won another match: increment wins and XP
UPDATE players
SET wins = 446,
    xp = 277400,
    kill_death_ratio = 2.36,
    last_login = '2026-03-18 01:15:00'
WHERE player_id = c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3;
