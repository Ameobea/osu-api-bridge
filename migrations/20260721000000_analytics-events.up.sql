CREATE TABLE IF NOT EXISTS analytics_events (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  project VARCHAR(64) NOT NULL,
  category VARCHAR(128) NOT NULL,
  subcategory VARCHAR(128) NOT NULL DEFAULT '',
  payload JSON NULL,
  session_id VARCHAR(32) NULL,
  ip_hash CHAR(16) NULL,
  user_agent VARCHAR(255) NULL,
  KEY idx_project_ts (project, ts),
  KEY idx_project_category_ts (project, category, ts)
);
