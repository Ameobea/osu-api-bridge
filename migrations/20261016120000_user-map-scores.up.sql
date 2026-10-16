CREATE TABLE IF NOT EXISTS user_map_scores (
  score_id BIGINT UNSIGNED NOT NULL PRIMARY KEY COMMENT 'osu! score id (solo score) from GET /beatmaps/{b}/scores/users/{u}/all',
  user_id INT UNSIGNED NOT NULL COMMENT 'player osu! user id',
  beatmap_id INT UNSIGNED NOT NULL COMMENT 'beatmap the score was set on',
  ruleset TINYINT NOT NULL COMMENT 'ruleset id (0 = osu!standard)',
  mods VARCHAR(255) NOT NULL COMMENT 'mod acronyms sorted and concatenated, e.g. DTHD (CL marks stable scores); same format as user_plays.mods',
  pp DOUBLE NULL COMMENT 'pp as reported by the API (NULL for unranked / loved maps)',
  accuracy DOUBLE NOT NULL COMMENT 'accuracy 0..1',
  max_combo INT NOT NULL COMMENT 'highest combo in the play',
  total_score BIGINT NOT NULL COMMENT 'standardised total score',
  `rank` VARCHAR(3) NOT NULL COMMENT 'letter grade',
  passed TINYINT(1) NOT NULL COMMENT '1 = pass',
  is_lazer TINYINT(1) NOT NULL COMMENT '1 when submitted by lazer (build_id present)',
  legacy_score_id BIGINT UNSIGNED NULL COMMENT 'stable score id when the score came from stable',
  ended_at DATETIME NOT NULL COMMENT 'UTC time the play ended',
  source VARCHAR(16) NOT NULL COMMENT 'writer of the last fetch: collector (recs-app priority phase) or bridge (/users/{u}/beatmaps/{b}/best and /scores)',
  fetched_at DATETIME NOT NULL COMMENT 'UTC time of the last fetch that returned this score',
  raw_json_gz MEDIUMBLOB NOT NULL COMMENT 'gzipped API score object without the embedded user, beatmapset and current_user_attributes',
  KEY user_map (user_id, beatmap_id),
  KEY beatmap_id (beatmap_id)
) COMMENT = 'Preserved scores (in practice the best per mod combination) of a user on a beatmap. Rows are kept when a later fetch no longer returns them. Written by the per-user collector and osu-api-bridge. Created 2026-10-16';

CREATE TABLE IF NOT EXISTS user_map_score_checks (
  user_id INT UNSIGNED NOT NULL COMMENT 'player osu! user id',
  beatmap_id INT UNSIGNED NOT NULL COMMENT 'beatmap checked',
  passed TINYINT(1) NULL COMMENT '1 / 0 from GET /users/{u}/beatmaps-passed (any mods, diff reduction included); NULL = never checked',
  passed_checked_at DATETIME NULL COMMENT 'UTC time of the last passed check',
  scores_fetched_at DATETIME NULL COMMENT 'UTC time of the last /scores/users/{u}/all fetch, by either writer',
  n_scores INT NULL COMMENT 'number of scores that fetch returned (0 = no preserved score)',
  PRIMARY KEY (user_id, beatmap_id)
) COMMENT = 'Per user and beatmap: whether and when user_map_scores was last checked, so empty results (never passed) are not refetched. Written by the per-user collector and osu-api-bridge. Created 2026-10-16';
