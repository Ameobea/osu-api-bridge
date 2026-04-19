use axum::{
  extract::{Path, Query},
  http::StatusCode,
  Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Transaction};

use crate::{
  db::db_pool,
  osu_api::{
    fetch_user_hiscores, fetch_v2_user_for_osutrack, HiscoreV1, MatchmakingStat, Ruleset,
    V2UserForOsutrack,
  },
};

use super::APIError;

#[derive(Deserialize)]
pub(super) struct OsutrackUpdateQuery {
  pub mode: Ruleset,
}

#[derive(Serialize)]
pub(super) struct UserInfo {
  pub osu_id: i64,
  pub username: String,
  pub country: String,
}

/// Snapshot of a row from the `updates` table in the legacy shape PHP used to emit directly from
/// mysqli — every numeric field is a string. Consumers of `api/get_user.php` etc. have been
/// receiving these values as strings for years; preserve that.
#[derive(Clone, Serialize)]
pub(super) struct LegacySnapshot {
  pub id: String,
  pub user: String,
  pub count300: String,
  pub count100: String,
  pub count50: String,
  pub playcount: String,
  pub ranked_score: String,
  pub total_score: String,
  pub pp_rank: String,
  pub level: String,
  pub pp_raw: String,
  pub accuracy: String,
  pub count_rank_ss: String,
  pub count_rank_s: String,
  pub count_rank_a: String,
  pub timestamp: String,
  pub mode: String,
}

#[derive(Serialize)]
pub(super) struct UpdateDiff {
  pub playcount: i64,
  pub pp_rank: i64,
  pub pp_raw: f64,
  pub accuracy: f64,
  pub total_score: i64,
  pub ranked_score: i64,
  pub count300: i64,
  pub count100: i64,
  pub count50: i64,
  pub level: f64,
  pub count_rank_a: i64,
  pub count_rank_s: i64,
  pub count_rank_ss: i64,
  pub levelup: bool,
  pub first: bool,
}

#[derive(Serialize)]
pub(super) struct NewHiscore {
  #[serde(flatten)]
  pub hiscore: HiscoreV1,
  pub ranking: usize,
}

#[derive(Serialize)]
pub(super) struct MatchmakingBlock {
  pub current: Vec<MatchmakingStat>,
  pub inserted_rows: usize,
}

#[derive(Serialize)]
pub(super) struct OsutrackUpdateResponse {
  pub exists: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub user: Option<UserInfo>,
  pub mode: u8,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub previous_snapshot: Option<LegacySnapshot>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub current_snapshot: Option<LegacySnapshot>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub diff: Option<UpdateDiff>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub new_hiscores: Option<Vec<NewHiscore>>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub matchmaking: Option<MatchmakingBlock>,
}

/// Everything we need from an `updates` row to compute the diff and produce the legacy snapshot.
/// All integer columns are cast to SIGNED in the query to avoid sqlx decode surprises caused by
/// differing column widths (`INT` vs `BIGINT`), and floats are cast to DOUBLE.
#[derive(sqlx::FromRow, Clone)]
struct UpdateRow {
  id: i64,
  user: i64,
  count300: i64,
  count100: i64,
  count50: i64,
  playcount: i64,
  ranked_score: i64,
  total_score: i64,
  pp_rank: i64,
  level: f64,
  pp_raw: f64,
  accuracy: f64,
  count_rank_ss: i64,
  count_rank_s: i64,
  count_rank_a: i64,
  timestamp: chrono::NaiveDateTime,
  mode: i64,
}

impl UpdateRow {
  fn to_snapshot(&self) -> LegacySnapshot {
    LegacySnapshot {
      id: self.id.to_string(),
      user: self.user.to_string(),
      count300: self.count300.to_string(),
      count100: self.count100.to_string(),
      count50: self.count50.to_string(),
      playcount: self.playcount.to_string(),
      ranked_score: self.ranked_score.to_string(),
      total_score: self.total_score.to_string(),
      pp_rank: self.pp_rank.to_string(),
      level: format_decimal_like_mysqli(self.level),
      pp_raw: format_decimal_like_mysqli(self.pp_raw),
      accuracy: format_decimal_like_mysqli(self.accuracy),
      count_rank_ss: self.count_rank_ss.to_string(),
      count_rank_s: self.count_rank_s.to_string(),
      count_rank_a: self.count_rank_a.to_string(),
      timestamp: self.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
      mode: self.mode.to_string(),
    }
  }
}

/// Format a float the way MySQL's DECIMAL-as-string looks on the wire. We don't have the exact
/// column scale here, so fall back to the shortest round-trippable representation; callers have
/// historically tolerated either form.
fn format_decimal_like_mysqli(v: f64) -> String {
  if v.fract() == 0.0 && v.abs() < 1e15 {
    format!("{}", v as i64)
  } else {
    format!("{}", v)
  }
}

const SELECT_UPDATES_COLS: &str = "CAST(id AS SIGNED) AS id, CAST(user AS SIGNED) AS user, \
  CAST(count300 AS SIGNED) AS count300, CAST(count100 AS SIGNED) AS count100, \
  CAST(count50 AS SIGNED) AS count50, CAST(playcount AS SIGNED) AS playcount, \
  CAST(ranked_score AS SIGNED) AS ranked_score, CAST(total_score AS SIGNED) AS total_score, \
  CAST(pp_rank AS SIGNED) AS pp_rank, CAST(level AS DOUBLE) AS level, \
  CAST(pp_raw AS DOUBLE) AS pp_raw, CAST(accuracy AS DOUBLE) AS accuracy, \
  CAST(count_rank_ss AS SIGNED) AS count_rank_ss, CAST(count_rank_s AS SIGNED) AS count_rank_s, \
  CAST(count_rank_a AS SIGNED) AS count_rank_a, CAST(timestamp AS DATETIME) AS timestamp, \
  CAST(mode AS SIGNED) AS mode";

#[derive(Clone, Copy)]
struct DerivedSnapshotFields {
  count300: i64,
  count100: i64,
  count50: i64,
  playcount: i64,
  ranked_score: i64,
  total_score: i64,
  pp_rank: i64,
  level: f64,
  pp_raw: f64,
  accuracy: f64,
  count_rank_ss: i64,
  count_rank_s: i64,
  count_rank_a: i64,
}

impl DerivedSnapshotFields {
  fn from_v2(u: &V2UserForOsutrack) -> Self {
    let s = &u.statistics;
    Self {
      count300: s.count_300 as i64,
      count100: s.count_100 as i64,
      count50: s.count_50 as i64,
      playcount: s.play_count as i64,
      ranked_score: s.ranked_score as i64,
      total_score: s.total_score as i64,
      pp_rank: s.global_rank.unwrap_or(0) as i64,
      level: s.level.current as f64 + s.level.progress as f64 / 100.0,
      pp_raw: s.pp,
      accuracy: s.hit_accuracy,
      count_rank_ss: s.grade_counts.ss + s.grade_counts.ssh,
      count_rank_s: s.grade_counts.s + s.grade_counts.sh,
      count_rank_a: s.grade_counts.a,
    }
  }

  /// True when `row` has identical stat values to these derived fields. Matches the PHP
  /// `$statsMatch` comparison — everything except timestamp, id, user, mode.
  fn matches(&self, row: &UpdateRow) -> bool {
    row.count300 == self.count300
      && row.count100 == self.count100
      && row.count50 == self.count50
      && row.playcount == self.playcount
      && row.ranked_score == self.ranked_score
      && row.total_score == self.total_score
      && row.pp_rank == self.pp_rank
      && row.level == self.level
      && row.pp_raw == self.pp_raw
      && row.accuracy == self.accuracy
      && row.count_rank_ss == self.count_rank_ss
      && row.count_rank_s == self.count_rank_s
      && row.count_rank_a == self.count_rank_a
  }
}

fn rows_match(a: &UpdateRow, b: &UpdateRow) -> bool {
  a.count300 == b.count300
    && a.count100 == b.count100
    && a.count50 == b.count50
    && a.playcount == b.playcount
    && a.ranked_score == b.ranked_score
    && a.total_score == b.total_score
    && a.pp_rank == b.pp_rank
    && a.level == b.level
    && a.pp_raw == b.pp_raw
    && a.accuracy == b.accuracy
    && a.count_rank_ss == b.count_rank_ss
    && a.count_rank_s == b.count_rank_s
    && a.count_rank_a == b.count_rank_a
}

fn internal_error(msg: &'static str) -> APIError {
  APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: msg.to_owned(),
  }
}

/// Port of the dedup-or-insert logic in `Classes/dbQuery.php::update()`. Selects the two most
/// recent `updates` rows before the write so we have both the "previous" snapshot for diff and
/// the 2-row window for the `$statsMatch` check. Returns `(previous_row, current_row)`.
async fn apply_updates_row(
  txn: &mut Transaction<'_, MySql>,
  osu_id: i64,
  mode_val: u8,
  derived: DerivedSnapshotFields,
) -> Result<(Option<UpdateRow>, Option<UpdateRow>), APIError> {
  let select_recent = format!(
    "SELECT {SELECT_UPDATES_COLS} FROM updates WHERE user = ? AND mode = ? ORDER BY timestamp \
     DESC LIMIT 2"
  );
  let recent: Vec<UpdateRow> = sqlx::query_as(&select_recent)
    .bind(osu_id)
    .bind(mode_val)
    .fetch_all(&mut **txn)
    .await
    .map_err(|err| {
      error!("osutrack_update: failed fetching recent updates rows: {err}");
      internal_error("Failed to fetch recent updates rows")
    })?;

  let previous = recent.first().cloned();

  // PHP skips the `updates` insert entirely when pp_rank == 0. Preserve that.
  if derived.pp_rank == 0 {
    return Ok((previous.clone(), previous));
  }

  let last = recent.first();
  let second_last = recent.get(1);
  let should_bump_timestamp = match (last, second_last) {
    (Some(last), Some(second_last)) => {
      rows_match(last, second_last) && derived.matches(last)
    },
    _ => false,
  };

  let current = if should_bump_timestamp {
    let last = last.unwrap();
    sqlx::query("UPDATE updates SET timestamp = NOW() WHERE id = ?")
      .bind(last.id)
      .execute(&mut **txn)
      .await
      .map_err(|err| {
        error!("osutrack_update: failed bumping updates.timestamp: {err}");
        internal_error("Failed to update updates.timestamp")
      })?;
    // Re-read the row so `timestamp` reflects NOW() in the response.
    let select_by_id = format!("SELECT {SELECT_UPDATES_COLS} FROM updates WHERE id = ?");
    let refreshed: Option<UpdateRow> = sqlx::query_as(&select_by_id)
      .bind(last.id)
      .fetch_optional(&mut **txn)
      .await
      .map_err(|err| {
        error!("osutrack_update: failed re-reading updates row: {err}");
        internal_error("Failed to re-read updates row")
      })?;
    refreshed
  } else {
    let insert_res = sqlx::query(
      "INSERT INTO updates (user, count300, count100, count50, playcount, ranked_score, \
       total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
       mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(osu_id)
    .bind(derived.count300)
    .bind(derived.count100)
    .bind(derived.count50)
    .bind(derived.playcount)
    .bind(derived.ranked_score)
    .bind(derived.total_score)
    .bind(derived.pp_rank)
    .bind(derived.level)
    .bind(derived.pp_raw)
    .bind(derived.accuracy)
    .bind(derived.count_rank_ss)
    .bind(derived.count_rank_s)
    .bind(derived.count_rank_a)
    .bind(mode_val)
    .execute(&mut **txn)
    .await
    .map_err(|err| {
      error!("osutrack_update: failed inserting updates row: {err}");
      internal_error("Failed to insert updates row")
    })?;
    let new_id = insert_res.last_insert_id() as i64;
    let select_by_id = format!("SELECT {SELECT_UPDATES_COLS} FROM updates WHERE id = ?");
    sqlx::query_as(&select_by_id)
      .bind(new_id)
      .fetch_optional(&mut **txn)
      .await
      .map_err(|err| {
        error!("osutrack_update: failed re-reading new updates row: {err}");
        internal_error("Failed to re-read new updates row")
      })?
  };

  Ok((previous, current))
}

/// Port of the hiscore dedup-and-insert loop in `Classes/dbQuery.php::update()`. A hiscore is
/// "new" if no existing `hiscore_updates` row for this user/mode matches on
/// `(beatmap_id, score, pp)`. Returns the new-hiscores list with `ranking` (the index in the
/// fetched top-100 list) matching PHP's output.
async fn diff_and_insert_hiscores(
  txn: &mut Transaction<'_, MySql>,
  osu_id: i64,
  mode_val: u8,
  hiscores: Vec<HiscoreV1>,
) -> Result<Vec<NewHiscore>, APIError> {
  #[derive(sqlx::FromRow)]
  struct ExistingHiscore {
    beatmap_id: i64,
    score: i64,
    pp: f64,
  }

  let existing: Vec<ExistingHiscore> = sqlx::query_as(
    "SELECT CAST(beatmap_id AS SIGNED) AS beatmap_id, CAST(score AS SIGNED) AS score, \
     CAST(pp AS DOUBLE) AS pp FROM hiscore_updates WHERE user = ? AND mode = ?",
  )
  .bind(osu_id)
  .bind(mode_val)
  .fetch_all(&mut **txn)
  .await
  .map_err(|err| {
    error!("osutrack_update: failed fetching existing hiscores: {err}");
    internal_error("Failed to fetch existing hiscores")
  })?;

  let mut new_hiscores: Vec<NewHiscore> = Vec::new();

  for (i, hs) in hiscores.into_iter().enumerate() {
    // PHP's `beatmap_id != 0` skip — a missing/deleted map slipping through as 0.
    let beatmap_id: i64 = hs.beatmap_id.parse().unwrap_or(0);
    if beatmap_id == 0 {
      continue;
    }
    let score: i64 = hs.score.parse().unwrap_or(0);
    let pp: f64 = hs.pp.parse().unwrap_or(0.0);

    let already_present = existing
      .iter()
      .any(|e| e.beatmap_id == beatmap_id && e.score == score && e.pp == pp);
    if already_present {
      continue;
    }

    let mods: i64 = hs.enabled_mods.parse().unwrap_or(0);
    let is_classic: i64 = if hs.is_classic == "1" { 1 } else { 0 };

    sqlx::query(
      "INSERT INTO hiscore_updates (user, beatmap_id, score, pp, mods, rank, score_time, mode, \
       is_classic) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE pp = VALUES(pp)",
    )
    .bind(osu_id)
    .bind(beatmap_id)
    .bind(score)
    .bind(pp)
    .bind(mods)
    .bind(&hs.rank)
    .bind(&hs.date)
    .bind(mode_val)
    .bind(is_classic)
    .execute(&mut **txn)
    .await
    .map_err(|err| {
      error!("osutrack_update: failed inserting hiscore: {err}");
      internal_error("Failed to insert hiscore")
    })?;

    new_hiscores.push(NewHiscore {
      hiscore: hs,
      ranking: i,
    });
  }

  Ok(new_hiscores)
}

async fn upsert_user_row(
  txn: &mut Transaction<'_, MySql>,
  osu_id: i64,
  username: &str,
) -> Result<(), APIError> {
  sqlx::query(
    "INSERT INTO users (osu_id, username, last_updated) VALUES (?, ?, NOW()) ON DUPLICATE KEY \
     UPDATE username = VALUES(username), last_updated = NOW()",
  )
  .bind(osu_id)
  .bind(username)
  .execute(&mut **txn)
  .await
  .map_err(|err| {
    error!("osutrack_update: failed upserting users row: {err}");
    internal_error("Failed to upsert users row")
  })?;
  Ok(())
}

/// Result of `write_matchmaking_updates` — how many rows we inserted (bumps don't count) and
/// whether the per-pool play counts changed vs. the most recent prior snapshot, which is what the
/// fetch-queue uses to decide if there's any point asking the worker to look again.
struct MatchmakingWriteResult {
  inserted: usize,
  plays_changed_or_new_pool: bool,
}

/// Upsert each pool we observed in this update into the `matchmaking_pools` side table. Pool
/// metadata (name, ruleset, variant, active flag) is stable and low-cardinality, so we keep it
/// here instead of duplicating it on every `matchmaking_updates` row.
async fn upsert_matchmaking_pools(
  txn: &mut Transaction<'_, MySql>,
  stats: &[MatchmakingStat],
) -> Result<(), APIError> {
  for stat in stats {
    sqlx::query(
      "INSERT INTO matchmaking_pools (id, name, ruleset_id, variant_id, active, last_seen) \
       VALUES (?, ?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE name = VALUES(name), \
       ruleset_id = VALUES(ruleset_id), variant_id = VALUES(variant_id), \
       active = VALUES(active), last_seen = NOW()",
    )
    .bind(stat.pool.id)
    .bind(&stat.pool.name)
    .bind(stat.pool.ruleset_id)
    .bind(stat.pool.variant_id)
    .bind(if stat.pool.active { 1i64 } else { 0 })
    .execute(&mut **txn)
    .await
    .map_err(|err| {
      error!("osutrack_update: failed upserting matchmaking_pools row: {err}");
      internal_error("Failed to upsert matchmaking_pools row")
    })?;
  }
  Ok(())
}

/// Writes each `matchmaking_stats` entry into `matchmaking_updates`, using the same dedup-or-insert
/// pattern as `updates` (compare the latest 2 rows for this (user, mode, pool); bump timestamp if
/// they match each other and the new data; otherwise INSERT). Also reports whether any pool's
/// `plays` increased since the last snapshot or a never-before-seen pool appeared, so the caller
/// can decide whether the fetch queue actually needs poking.
async fn write_matchmaking_updates(
  txn: &mut Transaction<'_, MySql>,
  osu_id: i64,
  mode_val: u8,
  stats: &[MatchmakingStat],
) -> Result<MatchmakingWriteResult, APIError> {
  #[derive(sqlx::FromRow, Clone)]
  struct MatchmakingRow {
    id: i64,
    rating: f64,
    rank: i64,
    plays: i64,
    first_placements: i64,
    total_points: i64,
    is_rating_provisional: i64,
  }

  let mut inserted: usize = 0;
  let mut plays_changed_or_new_pool = false;

  for stat in stats {
    let recent: Vec<MatchmakingRow> = sqlx::query_as(
      "SELECT CAST(id AS SIGNED) AS id, CAST(rating AS DOUBLE) AS rating, \
       CAST(`rank` AS SIGNED) AS `rank`, CAST(plays AS SIGNED) AS plays, \
       CAST(first_placements AS SIGNED) AS first_placements, \
       CAST(total_points AS SIGNED) AS total_points, \
       CAST(is_rating_provisional AS SIGNED) AS is_rating_provisional \
       FROM matchmaking_updates WHERE user = ? AND mode = ? AND pool_id = ? \
       ORDER BY timestamp DESC LIMIT 2",
    )
    .bind(osu_id)
    .bind(mode_val)
    .bind(stat.pool_id)
    .fetch_all(&mut **txn)
    .await
    .map_err(|err| {
      error!("osutrack_update: failed fetching recent matchmaking_updates rows: {err}");
      internal_error("Failed to fetch recent matchmaking_updates rows")
    })?;

    match recent.first() {
      None => plays_changed_or_new_pool = true,
      Some(last) if last.plays != stat.plays as i64 => plays_changed_or_new_pool = true,
      _ => {},
    }

    let new_is_provisional: i64 = if stat.is_rating_provisional { 1 } else { 0 };
    let new_rating = stat.rating as f64;
    let matches_new = |r: &MatchmakingRow| -> bool {
      r.rating == new_rating
        && r.rank == stat.rank as i64
        && r.plays == stat.plays as i64
        && r.first_placements == stat.first_placements as i64
        && r.total_points == stat.total_points
        && r.is_rating_provisional == new_is_provisional
    };
    let rows_equal = |a: &MatchmakingRow, b: &MatchmakingRow| -> bool {
      a.rating == b.rating
        && a.rank == b.rank
        && a.plays == b.plays
        && a.first_placements == b.first_placements
        && a.total_points == b.total_points
        && a.is_rating_provisional == b.is_rating_provisional
    };

    let should_bump = match (recent.first(), recent.get(1)) {
      (Some(last), Some(second_last)) => rows_equal(last, second_last) && matches_new(last),
      _ => false,
    };

    if should_bump {
      let last = recent.first().unwrap();
      sqlx::query("UPDATE matchmaking_updates SET timestamp = NOW() WHERE id = ?")
        .bind(last.id)
        .execute(&mut **txn)
        .await
        .map_err(|err| {
          error!("osutrack_update: failed bumping matchmaking_updates.timestamp: {err}");
          internal_error("Failed to update matchmaking_updates.timestamp")
        })?;
    } else {
      sqlx::query(
        "INSERT INTO matchmaking_updates (user, mode, pool_id, rating, `rank`, plays, \
         first_placements, total_points, is_rating_provisional) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
      .bind(osu_id)
      .bind(mode_val)
      .bind(stat.pool_id)
      .bind(stat.rating)
      .bind(stat.rank)
      .bind(stat.plays)
      .bind(stat.first_placements)
      .bind(stat.total_points)
      .bind(new_is_provisional)
      .execute(&mut **txn)
      .await
      .map_err(|err| {
        error!("osutrack_update: failed inserting matchmaking_updates row: {err}");
        internal_error("Failed to insert matchmaking_updates row")
      })?;
      inserted += 1;
    }
  }

  Ok(MatchmakingWriteResult {
    inserted,
    plays_changed_or_new_pool,
  })
}

async fn enqueue_fetch_queue(
  txn: &mut Transaction<'_, MySql>,
  osu_id: i64,
  mode_val: u8,
) -> Result<(), APIError> {
  sqlx::query(
    "INSERT INTO matchmaking_fetch_queue (user, mode, enqueued_at) VALUES (?, ?, NOW()) \
     ON DUPLICATE KEY UPDATE enqueued_at = VALUES(enqueued_at)",
  )
  .bind(osu_id)
  .bind(mode_val)
  .execute(&mut **txn)
  .await
  .map_err(|err| {
    error!("osutrack_update: failed upserting matchmaking_fetch_queue: {err}");
    internal_error("Failed to upsert matchmaking_fetch_queue")
  })?;
  Ok(())
}

fn compute_diff(previous: Option<&UpdateRow>, current: &UpdateRow, first: bool) -> UpdateDiff {
  // When there's no previous row, PHP effectively subtracts `null`, which coerces to 0. So the
  // diff equals the current snapshot.
  let (pcount, pprank, ppraw, pacc, ptotal, pranked, p300, p100, p50, plevel, pa, ps, pss) =
    match previous {
      Some(p) => (
        p.playcount,
        p.pp_rank,
        p.pp_raw,
        p.accuracy,
        p.total_score,
        p.ranked_score,
        p.count300,
        p.count100,
        p.count50,
        p.level,
        p.count_rank_a,
        p.count_rank_s,
        p.count_rank_ss,
      ),
      None => (0, 0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0),
    };

  let levelup = match previous {
    Some(p) => p.level.floor() != current.level.floor(),
    None => false,
  };

  UpdateDiff {
    playcount: current.playcount - pcount,
    pp_rank: current.pp_rank - pprank,
    pp_raw: current.pp_raw - ppraw,
    accuracy: current.accuracy - pacc,
    total_score: current.total_score - ptotal,
    ranked_score: current.ranked_score - pranked,
    count300: current.count300 - p300,
    count100: current.count100 - p100,
    count50: current.count50 - p50,
    level: current.level - plevel,
    count_rank_a: current.count_rank_a - pa,
    count_rank_s: current.count_rank_s - ps,
    count_rank_ss: current.count_rank_ss - pss,
    levelup,
    first,
  }
}

pub(super) async fn osutrack_update(
  Path(user): Path<String>,
  Query(OsutrackUpdateQuery { mode }): Query<OsutrackUpdateQuery>,
) -> Result<Json<OsutrackUpdateResponse>, APIError> {
  let mode_val = mode.mode_value();

  let Some(v2_user) = fetch_v2_user_for_osutrack(&user, mode).await? else {
    return Ok(Json(OsutrackUpdateResponse {
      exists: false,
      user: None,
      mode: mode_val,
      previous_snapshot: None,
      current_snapshot: None,
      diff: None,
      new_hiscores: None,
      matchmaking: None,
    }));
  };

  let osu_id = v2_user.id as i64;
  let derived = DerivedSnapshotFields::from_v2(&v2_user);

  // Fetch hiscores outside the transaction so we don't hold DB locks during a network round-trip.
  let hiscores_v2 = fetch_user_hiscores(v2_user.id, mode, Some(100), None).await?;
  let hiscores_v1: Vec<HiscoreV1> = hiscores_v2
    .into_iter()
    .map(|hs| hs.into_v1())
    .collect::<Result<_, _>>()
    .map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error converting hiscores to v1 format: {err}"),
    })?;

  let pool = db_pool();
  let mut txn = pool.begin().await.map_err(|err| {
    error!("osutrack_update: failed starting transaction: {err}");
    internal_error("Failed to start DB transaction")
  })?;

  let (previous_row, current_row) =
    apply_updates_row(&mut txn, osu_id, mode_val, derived).await?;

  let new_hiscores = diff_and_insert_hiscores(&mut txn, osu_id, mode_val, hiscores_v1).await?;
  upsert_user_row(&mut txn, osu_id, &v2_user.username).await?;

  upsert_matchmaking_pools(&mut txn, &v2_user.matchmaking_stats).await?;
  let mm_result =
    write_matchmaking_updates(&mut txn, osu_id, mode_val, &v2_user.matchmaking_stats).await?;
  // Only nudge the fetch queue when there's actually new match activity to ingest. Re-bumping the
  // queue on every refresh of an idle popular user wastes worker cycles later.
  if !v2_user.matchmaking_stats.is_empty() && mm_result.plays_changed_or_new_pool {
    enqueue_fetch_queue(&mut txn, osu_id, mode_val).await?;
  }

  txn.commit().await.map_err(|err| {
    error!("osutrack_update: failed committing transaction: {err}");
    internal_error("Failed to commit DB transaction")
  })?;

  let first = previous_row.is_none();
  let diff = current_row
    .as_ref()
    .map(|cur| compute_diff(previous_row.as_ref(), cur, first));

  Ok(Json(OsutrackUpdateResponse {
    exists: true,
    user: Some(UserInfo {
      osu_id,
      username: v2_user.username,
      country: v2_user.country_code,
    }),
    mode: mode_val,
    previous_snapshot: previous_row.as_ref().map(UpdateRow::to_snapshot),
    current_snapshot: current_row.as_ref().map(UpdateRow::to_snapshot),
    diff,
    new_hiscores: Some(new_hiscores),
    matchmaking: Some(MatchmakingBlock {
      current: v2_user.matchmaking_stats,
      inserted_rows: mm_result.inserted,
    }),
  }))
}
