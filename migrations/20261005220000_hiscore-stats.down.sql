SET SESSION lock_wait_timeout = 30;

ALTER TABLE hiscore_updates
  DROP COLUMN score_id, DROP COLUMN count_300, DROP COLUMN count_100, DROP COLUMN count_50,
  DROP COLUMN count_miss, DROP COLUMN max_combo, DROP COLUMN accuracy, DROP COLUMN perfect,
  DROP COLUMN statistics,
  ALGORITHM=INSTANT, LOCK=NONE;

ALTER TABLE hiscore_updates_deleted
  DROP COLUMN score_id, DROP COLUMN count_300, DROP COLUMN count_100, DROP COLUMN count_50,
  DROP COLUMN count_miss, DROP COLUMN max_combo, DROP COLUMN accuracy, DROP COLUMN perfect,
  DROP COLUMN statistics,
  ALGORITHM=INSTANT, LOCK=NONE;
