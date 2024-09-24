CREATE TABLE daily_challenge_rankings (
  day_id INT NOT NULL,
  user_id INT NOT NULL,
  score_id BIGINT NOT NULL,
  pp FLOAT NULL,
  rank VARCHAR(255) NULL,
  statistics JSON NULL,
  total_score INT NOT NULL,
  started_at TIMESTAMP NULL,
  mods JSON NULL,
  max_combo INT NOT NULL,
  accuracy FLOAT NOT NULL
);
-- pkey on (day_id, user_id)
ALTER TABLE daily_challenge_rankings ADD CONSTRAINT daily_challenge_rankings_pkey PRIMARY KEY (day_id, user_id);
