CREATE TABLE daily_challenge_embed (
  hash VARCHAR(16) NOT NULL,
  user_id INT NOT NULL,
  config JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (hash, user_id),
  INDEX daily_challenge_embed_user_id (user_id)
);
