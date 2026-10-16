-- Per-play statistics from the osu! API v2 score object; NULL for rows written before this
-- migration. Columns are appended and never reordered: osutrack/bestplays reads rows by position.
-- ALGORITHM=INSTANT makes MariaDB refuse (instead of rebuilding 175M rows) if it cannot do a
-- metadata-only change; lock_wait_timeout keeps the ALTER from queueing behind a long reader
-- (the nightly mysqldump) and stalling every query on the table.
SET SESSION lock_wait_timeout = 30;

ALTER TABLE hiscore_updates
  ADD COLUMN score_id BIGINT UNSIGNED NULL,
  ADD COLUMN count_300 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_100 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_50 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_miss MEDIUMINT UNSIGNED NULL,
  ADD COLUMN max_combo MEDIUMINT UNSIGNED NULL,
  ADD COLUMN accuracy FLOAT NULL,
  ADD COLUMN perfect TINYINT(1) NULL,
  ADD COLUMN statistics JSON NULL,
  ALGORITHM=INSTANT, LOCK=NONE;

ALTER TABLE hiscore_updates_deleted
  ADD COLUMN score_id BIGINT UNSIGNED NULL,
  ADD COLUMN count_300 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_100 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_50 MEDIUMINT UNSIGNED NULL,
  ADD COLUMN count_miss MEDIUMINT UNSIGNED NULL,
  ADD COLUMN max_combo MEDIUMINT UNSIGNED NULL,
  ADD COLUMN accuracy FLOAT NULL,
  ADD COLUMN perfect TINYINT(1) NULL,
  ADD COLUMN statistics JSON NULL,
  ALGORITHM=INSTANT, LOCK=NONE;
