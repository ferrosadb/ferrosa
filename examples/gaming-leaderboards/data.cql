USE gaming;

-- Player profiles
INSERT INTO players (player_id, username, display_name, level, xp, wins, losses, kill_death_ratio, created_at, last_login)
VALUES (a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, 'shadowfox', 'ShadowFox', 42, 185000, 312, 198, 1.85, '2025-01-15 10:00:00', '2026-03-17 20:30:00');

INSERT INTO players (player_id, username, display_name, level, xp, wins, losses, kill_death_ratio, created_at, last_login)
VALUES (b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, 'neonknight', 'NeonKnight', 38, 156000, 278, 222, 1.52, '2025-03-08 14:30:00', '2026-03-17 22:15:00');

INSERT INTO players (player_id, username, display_name, level, xp, wins, losses, kill_death_ratio, created_at, last_login)
VALUES (c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, 'pixelqueen', 'PixelQueen', 55, 274000, 445, 155, 2.34, '2024-11-20 09:00:00', '2026-03-18 01:00:00');

INSERT INTO players (player_id, username, display_name, level, xp, wins, losses, kill_death_ratio, created_at, last_login)
VALUES (d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, 'ironclad', 'Ironclad', 29, 98000, 189, 211, 1.12, '2025-06-01 16:00:00', '2026-03-16 18:45:00');

INSERT INTO players (player_id, username, display_name, level, xp, wins, losses, kill_death_ratio, created_at, last_login)
VALUES (e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, 'starbreaker', 'StarBreaker', 47, 210000, 368, 182, 2.01, '2025-02-14 11:00:00', '2026-03-17 23:30:00');

-- Ranked matches
INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('ranked', '2026-03-17 19:00:00', uuid(), 'Crimson Fortress', 1245, c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, ['shadowfox', 'neonknight', 'pixelqueen', 'ironclad']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('ranked', '2026-03-17 19:30:00', uuid(), 'Neon District', 980, a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, ['shadowfox', 'starbreaker', 'ironclad']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('ranked', '2026-03-17 20:15:00', uuid(), 'Skyfall Arena', 1560, e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, ['pixelqueen', 'starbreaker', 'neonknight', 'shadowfox']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('ranked', '2026-03-17 21:00:00', uuid(), 'Crimson Fortress', 1100, c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, ['pixelqueen', 'ironclad', 'starbreaker']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('ranked', '2026-03-17 22:00:00', uuid(), 'Neon District', 890, e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, ['shadowfox', 'neonknight', 'starbreaker']);

-- Casual matches
INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('casual', '2026-03-17 18:30:00', uuid(), 'Sunset Bay', 720, b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, ['neonknight', 'ironclad']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('casual', '2026-03-17 19:15:00', uuid(), 'Crystal Caves', 540, d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, ['ironclad', 'shadowfox', 'neonknight']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('casual', '2026-03-17 20:00:00', uuid(), 'Sunset Bay', 660, a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, ['shadowfox', 'pixelqueen']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('casual', '2026-03-17 21:30:00', uuid(), 'Crystal Caves', 480, e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, ['starbreaker', 'neonknight', 'ironclad']);

INSERT INTO matches (game_mode, started_at, match_id, map_name, duration_seconds, winner_id, players)
VALUES ('casual', '2026-03-17 22:30:00', uuid(), 'Sunset Bay', 600, c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, ['pixelqueen', 'starbreaker']);

-- ShadowFox's matches
INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, '2026-03-17 19:00:00', uuid(), 18, 12, 7, 2150, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, '2026-03-17 19:30:00', uuid(), 24, 8, 5, 3200, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, '2026-03-17 20:00:00', uuid(), 15, 6, 9, 2400, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, '2026-03-17 20:15:00', uuid(), 20, 14, 6, 2600, 'loss');

-- NeonKnight's matches
INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, '2026-03-17 18:30:00', uuid(), 14, 9, 3, 1850, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, '2026-03-17 19:00:00', uuid(), 12, 15, 8, 1600, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, '2026-03-17 19:15:00', uuid(), 8, 11, 4, 1200, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, '2026-03-17 22:00:00', uuid(), 16, 10, 5, 2050, 'loss');

-- PixelQueen's matches
INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, '2026-03-17 19:00:00', uuid(), 28, 7, 4, 3800, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, '2026-03-17 20:00:00', uuid(), 10, 8, 6, 1800, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, '2026-03-17 21:00:00', uuid(), 25, 9, 3, 3500, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, '2026-03-17 22:30:00', uuid(), 22, 5, 7, 3400, 'win');

-- Ironclad's matches
INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, '2026-03-17 18:30:00', uuid(), 9, 14, 6, 1100, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, '2026-03-17 19:00:00', uuid(), 11, 16, 5, 1350, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, '2026-03-17 19:15:00', uuid(), 16, 10, 8, 2100, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, '2026-03-17 21:00:00', uuid(), 13, 18, 4, 1500, 'loss');

-- StarBreaker's matches
INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, '2026-03-17 19:30:00', uuid(), 19, 11, 6, 2500, 'loss');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, '2026-03-17 20:15:00', uuid(), 26, 10, 4, 3300, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, '2026-03-17 21:30:00', uuid(), 21, 7, 9, 3100, 'win');

INSERT INTO player_matches (player_id, match_time, match_id, kills, deaths, assists, score, result)
VALUES (e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, '2026-03-17 22:00:00', uuid(), 22, 9, 5, 2900, 'win');

-- Ranked mode leaderboard
INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('ranked', 1, c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, 'pixelqueen', 274000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('ranked', 2, e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, 'starbreaker', 210000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('ranked', 3, a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, 'shadowfox', 185000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('ranked', 4, b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, 'neonknight', 156000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('ranked', 5, d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, 'ironclad', 98000);

-- Casual mode leaderboard
INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('casual', 1, e5e5e5e5-e5e5-e5e5-e5e5-e5e5e5e5e5e5, 'starbreaker', 142000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('casual', 2, c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3, 'pixelqueen', 138000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('casual', 3, a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1, 'shadowfox', 115000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('casual', 4, d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4, 'ironclad', 67000);

INSERT INTO leaderboard (game_mode, rank, player_id, username, score)
VALUES ('casual', 5, b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2, 'neonknight', 62000);
