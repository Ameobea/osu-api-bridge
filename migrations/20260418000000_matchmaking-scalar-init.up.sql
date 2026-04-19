CREATE TABLE matchmaking_pools (
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  ruleset_id TINYINT NOT NULL,
  variant_id INT NULL,
  active TINYINT(1) NOT NULL,
  last_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE matchmaking_updates (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user INT NOT NULL,
  mode TINYINT NOT NULL,
  pool_id INT NOT NULL,
  rating FLOAT NOT NULL,
  `rank` INT NOT NULL,
  plays INT NOT NULL,
  first_placements INT NOT NULL,
  total_points BIGINT NOT NULL,
  is_rating_provisional TINYINT(1) NOT NULL,
  timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_matchmaking_updates_user_mode_pool (user, mode, pool_id, timestamp),
  INDEX ix_matchmaking_updates_timestamp (timestamp)
);

CREATE TABLE matchmaking_fetch_queue (
  user INT NOT NULL,
  mode TINYINT NOT NULL,
  enqueued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_checked DATETIME NULL,
  PRIMARY KEY (user, mode),
  INDEX ix_matchmaking_fetch_queue_last_checked (last_checked)
);
