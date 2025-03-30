use std::cmp::Reverse;

use chrono::{DateTime, Datelike, Days, NaiveDate, Timelike, Utc, Weekday};
use fxhash::{FxHashMap, FxHashSet};
use serde::Serialize;
use sqlx::{mysql::MySqlRow, Executor, MySql, QueryBuilder, Row};
use tokio::sync::OnceCell;

use crate::{
  db::{conn, db_pool},
  osu_api::{
    self,
    daily_challenge::{DailyChallengeScore, NewDailyChallengeDescriptor},
    Mod,
  },
  util::serialize_json_bytes_opt,
};

use super::{admin::validate_admin_api_token, *};

async fn store_daily_challenge_metadata(
  txn: &mut sqlx::Transaction<'_, MySql>,
  metadata: &[NewDailyChallengeDescriptor],
) -> sqlx::Result<()> {
  if metadata.is_empty() {
    return Ok(());
  }

  let mut qb = QueryBuilder::new(
    "INSERT INTO daily_challenge_metadata (day_id, room_id, playlist_id, current_playlist_item) ",
  );

  qb.push_values(
    metadata,
    |mut b,
     NewDailyChallengeDescriptor {
       day_id,
       room_id,
       playlist_id,
       current_playlist_item,
     }| {
      b.push_bind(*day_id as i64)
        .push_bind(room_id)
        .push_bind(playlist_id)
        .push_bind(serde_json::to_string(current_playlist_item).unwrap());
    },
  );

  qb.push(
    "ON DUPLICATE KEY UPDATE current_playlist_item = VALUES(current_playlist_item), room_id = \
     VALUES(room_id), playlist_id = VALUES(playlist_id)",
  );
  let query = qb.build();

  txn.execute(query).await?;
  Ok(())
}

// CREATE TABLE daily_challenge_rankings (
//   day_id INT NOT NULL,
//   user_id INT NOT NULL,
//   score_id BIGINT NOT NULL,
//   pp FLOAT NULL,
//   rank VARCHAR(255) NULL,
//   statistics JSON NULL,
//   total_score INT NOT NULL,
//   started_at TIMESTAMP NULL,
//   mods JSON NULL,
//   max_combo INT NOT NULL,
//   accuracy FLOAT NOT NULL
// );
#[derive(Clone, Serialize)]
pub(crate) struct UserDailyChallengeScore {
  day_id: i64,
  user_id: i64,
  score_id: i64,
  pp: Option<f32>,
  rank: Option<String>,
  #[serde(serialize_with = "serialize_json_bytes_opt")]
  statistics: Option<Vec<u8>>,
  total_score: i64,
  started_at: Option<DateTime<Utc>>,
  ended_at: Option<DateTime<Utc>>,
  #[serde(serialize_with = "serialize_json_bytes_opt")]
  mods: Option<Vec<u8>>,
  max_combo: i64,
  accuracy: f32,
  user_rank: i64,
}

async fn build_and_run_user_daily_challenge_score_query(
  txn: &mut sqlx::Transaction<'_, MySql>,
  scores: Vec<UserDailyChallengeScore>,
) -> sqlx::Result<()> {
  let mut qb = QueryBuilder::new(
    "INSERT INTO daily_challenge_rankings (day_id, user_id, score_id, pp, rank, statistics, \
     total_score, started_at, ended_at, mods, max_combo, accuracy, user_rank) ",
  );

  qb.push_values(scores, |mut b, score| {
    b.push_bind(score.day_id)
      .push_bind(score.user_id)
      .push_bind(score.score_id)
      .push_bind(score.pp)
      .push_bind(score.rank)
      .push_bind(score.statistics)
      .push_bind(score.total_score)
      .push_bind(score.started_at)
      .push_bind(score.ended_at)
      .push_bind(score.mods)
      .push_bind(score.max_combo)
      .push_bind(score.accuracy)
      .push_bind(score.user_rank);
  });

  qb.push(
    "ON DUPLICATE KEY UPDATE pp = VALUES(pp), rank = VALUES(rank), statistics = \
     VALUES(statistics), total_score = VALUES(total_score), started_at = VALUES(started_at), mods \
     = VALUES(mods), max_combo = VALUES(max_combo), accuracy = VALUES(accuracy)",
  );
  let query = qb.build();
  txn.execute(query).await?;
  Ok(())
}

async fn store_daily_challenge_scores(
  day_id: usize,
  mut scores: Vec<DailyChallengeScore>,
  txn: &mut sqlx::Transaction<'_, MySql>,
) -> sqlx::Result<()> {
  scores.sort_unstable_by_key(|score| Reverse((score.total_score, Reverse(score.ended_at))));

  const CHUNK_SIZE: usize = 50;
  for (chunk_ix, scores) in scores.chunks(CHUNK_SIZE).enumerate() {
    let scores = scores
      .iter()
      .enumerate()
      .map(|(score_ix_in_chunk, score)| {
        let user_rank = chunk_ix * CHUNK_SIZE + score_ix_in_chunk + 1;

        UserDailyChallengeScore {
          day_id: day_id as _,
          user_id: score.user_id,
          score_id: score.id,
          pp: score.pp,
          rank: Some(score.rank.clone()),
          statistics: Some(serde_json::to_vec(&score.statistics).unwrap()),
          total_score: score.total_score as _,
          started_at: Some(score.started_at),
          ended_at: Some(score.ended_at),
          mods: Some(serde_json::to_vec(&score.mods).unwrap()),
          max_combo: score.max_combo as _,
          accuracy: score.accuracy,
          user_rank: user_rank as _,
        }
      })
      .collect::<Vec<_>>();

    build_and_run_user_daily_challenge_score_query(txn, scores).await?;
  }

  Ok(())
}

pub(super) async fn backfill_daily_challenges(
  Query(LoadUserTotalScoreRankingsQueryParams {
    fetch_missing_usernames,
    ..
  }): Query<LoadUserTotalScoreRankingsQueryParams>,
  admin_api_token: String,
) -> Result<(), APIError> {
  validate_admin_api_token(&admin_api_token)?;

  let mut all_daily_challenge_ids =
    osu_api::daily_challenge::get_daily_challenge_descriptors(false).await?;

  let mut txn = db_pool().begin().await.map_err(|err| {
    error!("Failed to start transaction for daily challenge backfill: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to start transaction for daily challenge backfill".to_owned(),
    }
  })?;

  let already_collected_day_ids: FxHashSet<usize> =
    sqlx::query_scalar!("SELECT day_id FROM daily_challenge_metadata;")
      .fetch_all(&mut *txn)
      .await
      .map_err(|err| {
        error!("Failed to fetch already collected daily challenge IDs from DB: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to fetch already collected daily challenge IDs from DB".to_owned(),
        }
      })?
      .into_iter()
      .map(|id| id as usize)
      .collect();

  all_daily_challenge_ids.retain(|ids| !already_collected_day_ids.contains(&ids.day_id));
  store_daily_challenge_metadata(&mut txn, &all_daily_challenge_ids)
    .await
    .map_err(|err| {
      error!("Failed to store daily challenge metadata in DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to store daily challenge metadata in DB".to_owned(),
      }
    })?;

  if !all_daily_challenge_ids.is_empty() {
    info!(
      "Found {} uncollected daily challenges to backfill",
      all_daily_challenge_ids.len(),
    );

    for ids in all_daily_challenge_ids {
      info!("Fetching daily challenge scores for {}...", ids.day_id);
      let scores = osu_api::daily_challenge::fetch_daily_challenge_scores(&ids).await?;
      info!(
        "Fetched {} scores for daily challenge {}.  Storing...",
        scores.len(),
        ids.day_id
      );

      match store_daily_challenge_scores(ids.day_id, scores, &mut txn).await {
        Ok(_) => info!(
          "Successfully stored daily challenge scores for {}",
          ids.day_id
        ),
        Err(err) => error!(
          "Failed to store daily challenge scores for {}: {err}",
          ids.day_id
        ),
      }
    }
  }

  txn.commit().await.map_err(|err| {
    error!("Failed to commit transaction for daily challenge backfill: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to commit transaction for daily challenge backfill".to_owned(),
    }
  })?;

  tokio::spawn(async move {
    info!("Refreshing daily challenge stats cache...");
    if let Some(stats_store) = DAILY_CHALLENGE_STATS.get() {
      match load_daily_challenge_stats().await {
        Ok(new_stats) => {
          stats_store.store(Arc::new(new_stats));
          info!("Successfully refreshed daily challenge stats cache");
        },
        Err(err) => {
          error!("Failed to refresh daily challenge stats cache: {err}");
        },
      }
    } else {
      match get_daily_challenge_stats().await {
        Ok(_) => info!("Successfully refreshed daily challenge stats cache"),
        Err(err) => error!("Failed to refresh daily challenge stats cache: {err:?}"),
      }
    }
  });

  tokio::spawn(async move {
    info!("Refreshing user total score rankings cache...");
    if let Some(rankings_store) = USER_TOTAL_SCORE_RANKINGS.get() {
      match load_user_total_score_rankings(fetch_missing_usernames.unwrap_or(true)).await {
        Ok(new_rankings) => {
          rankings_store.store(Arc::new(new_rankings));
          info!("Refreshed user total score rankings cache");
        },
        Err(err) => {
          error!("Failed to refresh user total score rankings cache: {err:?}");
        },
      }
    } else {
      match get_user_total_score_rankings(fetch_missing_usernames.unwrap_or(true)).await {
        Ok(_) => info!("Refreshed user total score rankings cache"),
        Err(err) => error!("Failed to refresh user total score rankings cache: {err:?}"),
      }
    }
  });

  tokio::spawn(async move {
    info!("Refreshing top 50% rankings cache...");
    if let Some(rankings_store) = USER_TOP_50_PERCENT_RANKINGS.get() {
      match load_user_top_n_percent_rankings(0.5).await {
        Ok(new_rankings) => {
          rankings_store.store(Arc::new(new_rankings));
          info!("Refreshed top 50% rankings cache");
        },
        Err(err) => {
          error!("Failed to refresh top 50% rankings cache: {err:?}");
        },
      }
    } else {
      match get_user_top_50_percent_rankings().await {
        Ok(_) => info!("Refreshed top 50% rankings cache"),
        Err(err) => error!("Failed to refresh top 50% rankings cache: {err:?}"),
      }
    }
  });

  tokio::spawn(async move {
    info!("Refreshing top 10% rankings cache...");
    if let Some(rankings_store) = USER_TOP_10_PERCENT_RANKINGS.get() {
      match load_user_top_n_percent_rankings(0.1).await {
        Ok(new_rankings) => {
          rankings_store.store(Arc::new(new_rankings));
          info!("Refreshed top 10% rankings cache");
        },
        Err(err) => {
          error!("Failed to refresh top 10% rankings cache: {err:?}");
        },
      }
    } else {
      match get_user_top_10_percent_rankings().await {
        Ok(_) => info!("Refreshed top 10% rankings cache"),
        Err(err) => error!("Failed to refresh top 10% rankings cache: {err:?}"),
      }
    }
  });

  tokio::spawn(async move {
    info!("Refreshing top 1% rankings cache...");
    if let Some(rankings_store) = USER_TOP_1_PERCENT_RANKINGS.get() {
      match load_user_top_n_percent_rankings(0.01).await {
        Ok(new_rankings) => {
          rankings_store.store(Arc::new(new_rankings));
          info!("Refreshed top 1% rankings cache");
        },
        Err(err) => {
          error!("Failed to refresh top 1% rankings cache: {err:?}");
        },
      }
    } else {
      match get_user_top_1_percent_rankings().await {
        Ok(_) => info!("Refreshed top 1% rankings cache"),
        Err(err) => error!("Failed to refresh top 1% rankings cache: {err:?}"),
      }
    }
  });

  tokio::spawn(async move {
    info!("Refreshing global daily challenge stats cache...");
    if let Some(global_stats) = GLOBAL_DAILY_CHALLENGE_STATS.get() {
      match load_global_daily_challenge_stats().await {
        Ok(new_stats) => {
          global_stats.store(Arc::new(new_stats));
          info!("Refreshed global daily challenge stats cache");
        },
        Err(err) => {
          error!("Failed to refresh global daily challenge stats cache: {err:?}");
        },
      }
    } else {
      match get_global_daily_challenge_stats().await {
        Ok(_) => info!("Refreshed global daily challenge stats cache"),
        Err(err) => error!("Failed to refresh global daily challenge stats cache: {err:?}"),
      }
    }
  });

  Ok(())
}

/// There was a bug computing `user_rank` in the `daily_challenge_rankings` table.  This endpoint
/// loads all daily challenge scores and recomputes `user_rank` for each score then saves them back.
pub(super) async fn recompute_all_user_ranks(admin_api_token: String) -> Result<(), APIError> {
  validate_admin_api_token(&admin_api_token)?;

  let mut txn = db_pool().begin().await.map_err(|err| {
    error!("Failed to start transaction for daily challenge user rank recomputation: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to start transaction for daily challenge user rank recomputation".to_owned(),
    }
  })?;

  let all_day_ids: Vec<i32> =
    sqlx::query_scalar!("SELECT DISTINCT day_id FROM daily_challenge_rankings")
      .fetch_all(&mut *txn)
      .await
      .map_err(|err| {
        error!("Failed to fetch all daily challenge IDs from DB: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to fetch all daily challenge IDs from DB".to_owned(),
        }
      })?;

  for day_id in all_day_ids {
    let mut scores: Vec<UserDailyChallengeScore> = sqlx::query_as!(
      UserDailyChallengeScore,
      "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, ended_at, \
       mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE day_id = ?",
      day_id
    )
    .fetch_all(&mut *txn)
    .await
    .map_err(|err| {
      error!("Failed to fetch daily challenge scores from DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch daily challenge scores from DB".to_owned(),
      }
    })?;

    scores.sort_unstable_by_key(|score| Reverse((score.total_score, Reverse(score.ended_at))));

    for (ix, score) in scores.iter_mut().enumerate() {
      score.user_rank = ix as i64 + 1;
    }

    // delete old scores for the day
    sqlx::query!(
      "DELETE FROM daily_challenge_rankings WHERE day_id = ?",
      day_id
    )
    .execute(&mut *txn)
    .await
    .map_err(|err| {
      error!("Failed to delete old daily challenge scores for day {day_id}: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to delete old daily challenge scores".to_owned(),
      }
    })?;

    const CHUNK_SIZE: usize = 50;
    for chunk in scores.chunks(CHUNK_SIZE) {
      // re-insert correctly sorted new ones
      let res = build_and_run_user_daily_challenge_score_query(&mut txn, chunk.to_owned()).await;
      if let Err(err) = res {
        error!("Failed to store daily challenge scores in DB: {err}");
        return Err(APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to store daily challenge scores in DB".to_owned(),
        });
      }
    }

    info!("Successfully recomputed user ranks for daily challenge {day_id}");
  }

  txn.commit().await.map_err(|err| {
    error!("Failed to commit transaction for daily challenge user rank recomputation: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to commit transaction for daily challenge user rank recomputation"
        .to_owned(),
    }
  })?;

  Ok(())
}

#[derive(Clone, Serialize, sqlx::FromRow)]
pub struct DbDailyChallengeDescriptor {
  pub day_id: i32,
  pub room_id: i64,
  pub playlist_id: i64,
  #[serde(serialize_with = "serialize_json_bytes_opt")]
  pub current_playlist_item: Option<Vec<u8>>,
}

async fn load_daily_challenge_stats() -> sqlx::Result<FxHashMap<usize, DailyChallengeStatsForDay>> {
  let pool = db_pool();
  let mut conn = pool.acquire().await?;

  let query =
    sqlx::query!("SELECT day_id, total_score AS total_scores FROM daily_challenge_rankings");
  let total_scores_rows = query.fetch_all(&mut *conn).await?;
  let scores_by_day_id: FxHashMap<usize, Vec<usize>> =
    total_scores_rows
      .into_iter()
      .fold(FxHashMap::default(), |mut map, row| {
        map
          .entry(row.day_id as usize)
          .or_insert_with(Vec::new)
          .push(row.total_scores as usize);
        map
      });

  let query = sqlx::query_as!(
    DbDailyChallengeDescriptor,
    "SELECT day_id, room_id, playlist_id, current_playlist_item FROM daily_challenge_metadata"
  );
  let metadata_rows = query.fetch_all(&mut *conn).await?;

  let mut stats = FxHashMap::default();
  for descriptor in metadata_rows {
    let total_scores = scores_by_day_id
      .get(&(descriptor.day_id as usize))
      .map(|scores| scores.len())
      .unwrap_or(0);
    let all_scores = scores_by_day_id.get(&(descriptor.day_id as usize));
    let histogram = all_scores
      .map(|scores| {
        let (min, max) = scores
          .iter()
          .fold((usize::MAX, usize::MIN), |(min, max), &score| {
            (min.min(score), max.max(score))
          });
        let bucket_count = 50;
        let bucket_size = (max - min) / bucket_count;
        let mut buckets = vec![0; bucket_count];
        for score in scores {
          let bucket = (score - min) / bucket_size;
          buckets[bucket.min(bucket_count - 1)] += 1;
        }

        Histogram {
          min: min as f32,
          max: max as f32,
          buckets,
        }
      })
      .unwrap_or_else(|| Histogram {
        min: 0.,
        max: 1_000_000.,
        buckets: vec![0, 0, 0],
      });

    stats.insert(descriptor.day_id as _, DailyChallengeStatsForDay {
      descriptor,
      total_scores,
      histogram,
    });
  }

  Ok(stats)
}

#[derive(Clone, Serialize)]
pub(crate) struct TopMapperEntry {
  pub user_id: usize,
  pub username: String,
  pub map_ids: Vec<usize>,
}

#[derive(Clone, Serialize)]
pub(crate) struct MinimalDailyChallengeDescriptor {
  pub day_id: usize,
  pub beatmap_id: u64,
  pub title: String,
  pub difficulty_name: String,
  pub mapper_user_id: u64,
  pub mapper_username: String,
  pub stars: f32,
  pub date_ranked: DateTime<Utc>,
  pub length_seconds: usize,
}

#[derive(Clone, Serialize)]
pub(crate) struct TopPPPlay {
  username: String,
  score: UserDailyChallengeScore,
  total_scores_for_day: usize,
}

#[derive(Clone, Serialize)]
pub(crate) struct MapStats {
  pub difficulty_distribution: Histogram,
  pub easiest_map: Option<MinimalDailyChallengeDescriptor>,
  pub hardest_map: Option<MinimalDailyChallengeDescriptor>,
  pub length_distribution_seconds: Histogram,
  pub shortest_map: Option<MinimalDailyChallengeDescriptor>,
  pub longest_map: Option<MinimalDailyChallengeDescriptor>,
  pub ranked_timestamp_distribution: Histogram,
  pub oldest_map: Option<MinimalDailyChallengeDescriptor>,
  pub newest_map: Option<MinimalDailyChallengeDescriptor>,
  pub ar_distribution: Histogram,
  pub od_distribution: Histogram,
  pub cs_distribution: Histogram,
  pub top_mappers: Vec<TopMapperEntry>,
  pub top_required_mods: Vec<(Vec<Mod>, usize)>,
  pub top_pp_plays: Vec<TopPPPlay>,
}

#[derive(Clone, Serialize)]
pub(crate) struct GlobalDailyChallengeStats {
  /// Total unique users who have participated in daily challenges
  pub total_unique_participants: usize,
  /// Total combined score of all users on all days
  pub total_combined_score: usize,
  /// Total number of rankings across all days
  pub total_rankings: usize,
  /// Total number of daily challenges
  pub total_challenges: usize,
  pub map_stats: MapStats,
}

fn build_histogram(
  data: Vec<f32>,
  bucket_count: usize,
  explicit_min: Option<f32>,
  explicit_max: Option<f32>,
) -> Histogram {
  let (computed_min, computed_max) = data
    .iter()
    .fold((f32::INFINITY, f32::NEG_INFINITY), |(min, max), &val| {
      (min.min(val), max.max(val))
    });
  let min = explicit_min.unwrap_or(computed_min);
  let max = explicit_max.unwrap_or(computed_max);
  let bucket_size = (max - min) / bucket_count as f32;

  let mut buckets = vec![0; bucket_count];
  for val in data {
    let bucket = ((val - min) / bucket_size) as usize;
    buckets[bucket.min(bucket_count - 1)] += 1;
  }

  Histogram { min, max, buckets }
}

struct MinMaxMapContainer<T: Clone, V: Copy + PartialOrd, F: Fn(&T) -> Option<V>> {
  min: Option<V>,
  max: Option<V>,
  min_value: Option<T>,
  max_value: Option<T>,
  get_value: F,
}

impl<T: Clone, V: Copy + PartialOrd, F: Fn(&T) -> Option<V>> MinMaxMapContainer<T, V, F> {
  fn new(get_value: F) -> Self {
    Self {
      min: None,
      max: None,
      min_value: None,
      max_value: None,
      get_value,
    }
  }

  fn insert(&mut self, map: T) {
    let Some(value) = (self.get_value)(&map) else {
      return;
    };

    if self.min.as_ref().map_or(true, |min| value < *min) {
      self.min = Some(value);
      self.min_value = Some(map.clone());
    }
    if self.max.as_ref().map_or(true, |max| value > *max) {
      self.max = Some(value);
      self.max_value = Some(map);
    }
  }

  fn into_inner(self) -> (Option<T>, Option<T>) { (self.min_value, self.max_value) }
}

async fn get_map_stats() -> Result<MapStats, APIError> {
  // check for missing beatmap metadata
  let missing_beatmap_ids = sqlx::query_scalar!(
    "SELECT
      DISTINCT CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.id') AS UNSIGNED)
    FROM daily_challenge_metadata
      INNER JOIN beatmaps
        ON beatmaps.beatmap_id=CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.id') AS \
     UNSIGNED)
    WHERE beatmaps.title IS NULL"
  )
  .fetch_all(db_pool())
  .await
  .map_err(|err| {
    error!("Failed to fetch missing beatmap IDs from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch missing beatmap IDs from DB".to_owned(),
    }
  })?;
  let missing_beatmap_ids = missing_beatmap_ids
    .into_iter()
    .filter_map(|id| id)
    .collect::<Vec<_>>();
  if !missing_beatmap_ids.is_empty() {
    warn!(
      "Found {} missing beatmap IDs in daily challenge metadata",
      missing_beatmap_ids.len()
    );

    let client = reqwest::Client::new();
    for id in missing_beatmap_ids {
      info!("Fetching beatmap id={id}...");
      match client
        .get(&format!(
          "https://ameobea.me/osutrack/getBeatmap.php?id={id}"
        ))
        .send()
        .await
      {
        Ok(res) =>
          if res.status().is_success() {
            info!("Successfully fetched beatmap id={id}");
          } else {
            error!("Failed to fetch beatmap id={id}: {}", res.status());
          },
        Err(err) => error!("Failed to fetch beatmap id={id}: {err}"),
      }
    }
  }

  let mut conn = conn().await?;
  let query = sqlx::query!(
    r#"
    SELECT
      day_id,
      CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.id') AS UNSIGNED) AS beatmap_id,
      CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.difficulty_rating') AS FLOAT) AS stars,
      CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.user_id') AS UNSIGNED) AS mapper_user_id,
      JSON_EXTRACT(current_playlist_item, '$.required_mods') AS required_mods,
      beatmaps.total_length AS length_seconds,
      beatmaps.diff_size AS cs,
      beatmaps.diff_approach AS ar,
      beatmaps.diff_overall AS od,
      beatmaps.approved_date AS date_ranked,
      beatmaps.title AS title,
      beatmaps.version AS difficulty_name,
      users.username AS mapper_username
    FROM daily_challenge_metadata
      INNER JOIN beatmaps ON beatmaps.beatmap_id=JSON_EXTRACT(current_playlist_item, '$.beatmap.id')
      LEFT JOIN users ON users.osu_id=CAST(JSON_EXTRACT(current_playlist_item, '$.beatmap.user_id') AS UNSIGNED)
    "#,
  );
  let rows = query.fetch_all(&mut *conn).await.map_err(|err| {
    error!("Failed to fetch daily challenge metadata from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch daily challenge metadata from DB".to_owned(),
    }
  })?;

  let mut difficulties = Vec::with_capacity(rows.len());
  let mut ranked_timestamps = Vec::with_capacity(rows.len());
  let mut lengths_seconds = Vec::with_capacity(rows.len());
  let mut ars = Vec::with_capacity(rows.len());
  let mut ods = Vec::with_capacity(rows.len());
  let mut css = Vec::with_capacity(rows.len());
  let mut top_mappers = FxHashMap::default();
  let mut top_required_mods = FxHashMap::default();

  let mut min_max_difficulties =
    MinMaxMapContainer::new(|row: &Arc<MinimalDailyChallengeDescriptor>| Some(row.stars));
  let mut min_max_lengths_seconds =
    MinMaxMapContainer::new(|row: &Arc<MinimalDailyChallengeDescriptor>| Some(row.length_seconds));
  let mut min_max_ranked_timestamps =
    MinMaxMapContainer::new(|row: &Arc<MinimalDailyChallengeDescriptor>| {
      Some(row.date_ranked.timestamp())
    });

  for row in rows {
    let Some(beatmap_id) = row.beatmap_id else {
      error!("Missing beatmap ID for daily challenge metadata row");
      continue;
    };

    let Some(stars) = row.stars else {
      error!("Missing difficulty rating for beatmap {beatmap_id}");
      continue;
    };
    difficulties.push(stars);

    let Some(date_ranked) = row.date_ranked else {
      error!("Missing ranked date for beatmap {beatmap_id}");
      continue;
    };
    ranked_timestamps.push(date_ranked.and_utc().timestamp() as f32);

    let Some(mapper_user_id) = row.mapper_user_id else {
      error!("Missing mapper user ID for beatmap {beatmap_id}");
      continue;
    };
    top_mappers
      .entry(mapper_user_id as usize)
      .or_insert_with(|| TopMapperEntry {
        user_id: mapper_user_id as usize,
        username: "Unknown".to_owned(),
        map_ids: Vec::new(),
      })
      .map_ids
      .push(beatmap_id as usize);

    lengths_seconds.push(row.length_seconds as f32);
    ars.push(row.ar as f32);
    ods.push(row.od as f32);
    css.push(row.cs as f32);

    let required_mods: Vec<Mod> = match row.required_mods {
      Some(required_mods) => serde_json::from_slice(&required_mods).unwrap_or_else(|err| {
        warn!("Failed to parse required mods for beatmap {beatmap_id}: {err}");
        Vec::new()
      }),
      None => Vec::new(),
    };
    let day_id = row.day_id as usize;
    let entry = top_required_mods
      .entry(required_mods)
      .or_insert((0usize, day_id));
    entry.0 += 1;
    entry.1 = entry.1.max(day_id);

    let minimal_desc = Arc::new(MinimalDailyChallengeDescriptor {
      day_id,
      beatmap_id,
      title: row.title,
      difficulty_name: row.difficulty_name,
      mapper_user_id,
      mapper_username: row.mapper_username.unwrap_or_else(|| "Unknown".to_owned()),
      stars,
      date_ranked: date_ranked.and_utc(),
      length_seconds: row.length_seconds as usize,
    });

    min_max_difficulties.insert(Arc::clone(&minimal_desc));
    min_max_lengths_seconds.insert(Arc::clone(&minimal_desc));
    min_max_ranked_timestamps.insert(minimal_desc);
  }

  let difficulty_distribution = build_histogram(difficulties, 20, None, None);
  let length_distribution_seconds = build_histogram(lengths_seconds, 20, None, None);
  let ranked_timestamp_distribution = build_histogram(ranked_timestamps, 20, None, None);
  let ar_distribution = build_histogram(ars, 11, Some(0.), Some(11.));
  let od_distribution = build_histogram(ods, 11, Some(0.), Some(11.));
  let cs_distribution = build_histogram(css, 10, Some(0.), Some(10.));

  let mut top_mappers: Vec<_> = top_mappers.into_values().collect();
  top_mappers.sort_unstable_by_key(|entry| Reverse(entry.map_ids.len()));
  top_mappers.retain(|entry| entry.map_ids.len() > 1);
  let count_at_ix_5 = top_mappers.get(5).map(|entry| entry.map_ids.len());
  let count_at_ix_20 = top_mappers.get(20).map(|entry| entry.map_ids.len());

  match (count_at_ix_5, count_at_ix_20) {
    (None, None) => (),
    (Some(count_at_ix_5), Some(count_at_ix_20)) =>
      if count_at_ix_5 != count_at_ix_20 {
        top_mappers.retain(|entry| entry.map_ids.len() >= count_at_ix_5);
      } else if top_mappers[0].map_ids.len() > count_at_ix_5 {
        top_mappers.retain(|entry| entry.map_ids.len() > count_at_ix_5);
      },
    (Some(count_at_ix_5), None) => {
      top_mappers.retain(|entry| entry.map_ids.len() >= count_at_ix_5);
    },
    _ => top_mappers.truncate(5),
  }

  // fetch usernames for top mappers
  let user_ids: Vec<usize> = top_mappers.iter().map(|entry| entry.user_id).collect();
  let (all_user_ids, missing_user_ids, _existing_user_ids) =
    get_existing_user_ids(&user_ids).await.map_err(|err| {
      error!("Failed to fetch existing user IDs from DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch existing user IDs from DB".to_owned(),
      }
    })?;

  populate_missing_usernames(missing_user_ids.iter().copied(), all_user_ids.len()).await;

  let mut qb = QueryBuilder::new("SELECT osu_id, username FROM users WHERE osu_id IN ");
  qb.push_tuples(user_ids.iter(), |mut b, &osu_id| {
    b.push_bind(osu_id as i64);
  });
  let query = qb.build();

  let res: Vec<MySqlRow> = query.fetch_all(&mut *conn).await.map_err(|err| {
    error!("Failed to fetch usernames for top mappers from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch usernames for top mappers from DB".to_owned(),
    }
  })?;
  let username_by_user_id: FxHashMap<usize, String> = res
    .into_iter()
    .map(|row| (row.get::<i64, _>(0) as usize, row.get(1)))
    .collect();

  for entry in &mut top_mappers {
    entry.username = username_by_user_id
      .get(&entry.user_id)
      .cloned()
      .unwrap_or_else(|| {
        warn!(
          "Missing username for user {} even trying to populate missing usernames",
          entry.user_id
        );
        "Unknown".to_owned()
      });
  }

  let mut top_required_mods: Vec<_> = top_required_mods.into_iter().collect();
  top_required_mods.sort_unstable_by_key(|(_, (count, most_recent_day_id))| {
    Reverse((*count, *most_recent_day_id))
  });
  let top_required_mods = top_required_mods
    .into_iter()
    .map(|(mods, (count, _))| (mods, count))
    .collect();

  let (easiest_map, hardest_map) = min_max_difficulties.into_inner();
  let (shortest_map, longest_map) = min_max_lengths_seconds.into_inner();
  let (oldest_map, newest_map) = min_max_ranked_timestamps.into_inner();

  let top_pp_plays: Vec<UserDailyChallengeScore> = sqlx::query_as!(
    UserDailyChallengeScore,
    "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, ended_at, \
     mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings ORDER BY pp DESC LIMIT 20"
  )
  .fetch_all(db_pool())
  .await
  .map_err(|err| {
    error!("Failed to fetch top PP plays from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch top PP plays from DB".to_owned(),
    }
  })?;

  // fetch usernames in a separate query because I don't want to create a new struct
  let user_ids: Vec<usize> = top_pp_plays
    .iter()
    .map(|score| score.user_id as usize)
    .collect();
  let mut qb = QueryBuilder::new("SELECT osu_id, username FROM users WHERE osu_id IN ");
  qb.push_tuples(user_ids.iter(), |mut b, &osu_id| {
    b.push_bind(osu_id as i64);
  });
  let query = qb.build();
  let top_pp_usernames_res: Vec<MySqlRow> = query.fetch_all(&mut *conn).await.map_err(|err| {
    error!("Failed to fetch usernames for top PP plays from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch usernames for top PP plays from DB".to_owned(),
    }
  })?;
  let username_by_user_id: FxHashMap<usize, String> = top_pp_usernames_res
    .into_iter()
    .map(|row| (row.get::<i64, _>(0) as usize, row.get(1)))
    .collect();

  let counts_by_day = get_daily_challenge_stats().await?.load();
  let top_pp_plays = top_pp_plays
    .into_iter()
    .map(|score| {
      let total_scores_for_day = counts_by_day
        .get(&(score.day_id as usize))
        .map(|stats| stats.total_scores)
        .unwrap_or(0);
      TopPPPlay {
        username: username_by_user_id
          .get(&(score.user_id as usize))
          .cloned()
          .unwrap_or_else(|| {
            warn!("Missing username for user {}", score.user_id);
            "Unknown".to_owned()
          }),
        score,
        total_scores_for_day,
      }
    })
    .collect();

  Ok(MapStats {
    difficulty_distribution,
    length_distribution_seconds,
    ranked_timestamp_distribution,
    ar_distribution,
    od_distribution,
    cs_distribution,
    top_mappers,
    top_required_mods,
    easiest_map: easiest_map.map(|desc| (*desc).clone()),
    hardest_map: hardest_map.map(|desc| (*desc).clone()),
    shortest_map: shortest_map.map(|desc| (*desc).clone()),
    longest_map: longest_map.map(|desc| (*desc).clone()),
    oldest_map: oldest_map.map(|desc| (*desc).clone()),
    newest_map: newest_map.map(|desc| (*desc).clone()),
    top_pp_plays,
  })
}

async fn load_global_daily_challenge_stats() -> Result<GlobalDailyChallengeStats, APIError> {
  let mut conn = conn().await?;
  let (total_combined_score, total_rankings, total_unique_participants, total_challenges) =
    sqlx::query!(
      r#"
      SELECT CAST(SUM(total_score) as UNSIGNED) AS total_combined_score, COUNT(*) AS total_rankings, COUNT(DISTINCT user_id) AS total_unique_participants, COUNT(DISTINCT day_id) AS total_challenges
      FROM daily_challenge_rankings
      "#,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| {
      error!("Failed to fetch global daily challenge stats from DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch global daily challenge stats from DB".to_owned(),
      }
    })
    .map(|row| {
      (
        row.total_combined_score.unwrap_or(0) as usize,
        row.total_rankings as usize,
        row.total_unique_participants as usize,
        row.total_challenges as usize,
      )
    })?;

  let map_stats = get_map_stats().await?;

  Ok(GlobalDailyChallengeStats {
    total_combined_score,
    total_rankings,
    total_unique_participants,
    total_challenges,
    map_stats,
  })
}

async fn get_existing_user_ids(
  user_ids: &[usize],
) -> sqlx::Result<(FxHashSet<usize>, Vec<usize>, FxHashSet<usize>)> {
  let mut qb = QueryBuilder::new("SELECT osu_id FROM users WHERE osu_id IN ");
  qb.push_tuples(user_ids, |mut b, &osu_id| {
    b.push_bind(osu_id as i64);
  });
  let query = qb.build();

  let res: Vec<MySqlRow> = query.fetch_all(db_pool()).await?;
  let all_user_ids: FxHashSet<usize> = user_ids.iter().copied().collect();
  let existing_user_ids: FxHashSet<usize> = res
    .into_iter()
    .map(|row| sqlx::Row::get::<i64, _>(&row, 0usize) as usize)
    .collect();
  let missing_user_ids: Vec<usize> = all_user_ids
    .difference(&existing_user_ids)
    .copied()
    .collect();

  Ok((all_user_ids, missing_user_ids, existing_user_ids))
}

async fn populate_missing_usernames(
  missing_user_ids: impl ExactSizeIterator<Item = usize>,
  total_user_ids: usize,
) {
  if missing_user_ids.len() == 0 {
    return;
  }

  info!(
    "Missing {}/{total_user_ids} usernames; fetching...",
    missing_user_ids.len(),
  );

  let client = reqwest::Client::new();
  for id in missing_user_ids {
    let url = format!("https://osutrack-api.ameo.dev/update?user={id}&mode=0");
    let req = client.post(&url).send().await;
    match req {
      Ok(res) =>
        if res.status().is_success() {
          info!("Successfully updated user {id}");
        } else {
          error!("Failed to update user {id}: {}", res.status());
        },
      Err(err) => error!("Failed to update user {id}: {err}"),
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
  }
}

async fn build_rankings(
  user_ids: &[usize],
  fetch_missing_usernames: bool,
) -> Result<Vec<DailyChallengeTotalScoreRankingEntry>, APIError> {
  let mut conn = conn().await?;

  let (all_user_ids, missing_user_ids, _existing_user_ids) =
    get_existing_user_ids(user_ids).await.map_err(|err| {
      error!("Failed to fetch existing user IDs from DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch existing user IDs from DB".to_owned(),
      }
    })?;

  if fetch_missing_usernames {
    populate_missing_usernames(missing_user_ids.iter().copied(), all_user_ids.len()).await;
  }

  let rankings_query = sqlx::query!(
    r#"
    WITH total_scores AS (
      SELECT user_id, SUM(total_score) AS total_score
      FROM daily_challenge_rankings
      GROUP BY user_id
    )
    SELECT user_id, username, CAST(total_score AS UNSIGNED) AS total_score
    FROM total_scores
    LEFT JOIN users ON users.osu_id = total_scores.user_id
    ORDER BY total_score DESC
    "#,
  );
  let rankings = rankings_query.fetch_all(&mut *conn).await.map_err(|err| {
    error!("Failed to fetch daily challenge rankings from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch daily challenge rankings from DB".to_owned(),
    }
  })?;

  let rankings: Vec<DailyChallengeTotalScoreRankingEntry> = rankings
    .into_iter()
    .enumerate()
    .map(|(rank, row)| DailyChallengeTotalScoreRankingEntry {
      user_id: row.user_id as u64,
      username: row.username.unwrap_or_else(|| "Unknown".to_owned()),
      rank: rank as u64 + 1,
      total_score: row.total_score.unwrap_or(0),
    })
    .collect();
  Ok(rankings)
}

#[derive(Deserialize)]
pub(crate) struct LoadUserTotalScoreRankingsQueryParams {
  fetch_missing_usernames: Option<bool>,
  page: Option<usize>,
}

async fn load_user_total_score_rankings(
  fetch_missing_usernames: bool,
) -> Result<UserTotalScoreRankings, APIError> {
  let query = sqlx::query_scalar!(
    "SELECT user_id FROM daily_challenge_rankings GROUP BY user_id ORDER BY SUM(total_score) DESC"
  );
  let rows = query.fetch_all(db_pool()).await.map_err(|err| {
    error!("Failed to load user total score rankings from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load user total score rankings from DB".to_owned(),
    }
  })?;

  let mut rank_by_user_id = FxHashMap::default();
  for (zero_indexed_rank, user_id) in rows.into_iter().enumerate() {
    let rank = zero_indexed_rank + 1;
    rank_by_user_id.insert(user_id as usize, rank);
  }

  let user_ids: Vec<usize> = rank_by_user_id.keys().copied().collect();
  let rankings: Vec<DailyChallengeTotalScoreRankingEntry> =
    build_rankings(&user_ids, fetch_missing_usernames).await?;

  Ok(UserTotalScoreRankings {
    rank_by_user_id,
    rankings,
  })
}

async fn load_user_top_n_percent_rankings(percent: f32) -> Result<UserPercentRankings, APIError> {
  struct PercentileRankEntry {
    pub user_id: u64,
    pub username: Option<String>,
    pub top_percent_count: u64,
  }

  let entries = sqlx::query_as!(
    PercentileRankEntry,
    r#"
    WITH
    total_user_counts AS (
      SELECT day_id, COUNT(DISTINCT(user_id)) as total_user_count
      FROM daily_challenge_rankings
      GROUP BY day_id
    ),
    user_total_scores AS (
      SELECT user_id, SUM(total_score) as user_total_score_sum
      FROM daily_challenge_rankings
      GROUP BY user_id
    ),
    top_counts AS (
      SELECT daily_challenge_rankings.user_id, COUNT(*) as top_percent_count, user_total_score_sum
      FROM daily_challenge_rankings
      INNER JOIN total_user_counts ON total_user_counts.day_id = daily_challenge_rankings.day_id
      INNER JOIN user_total_scores ON user_total_scores.user_id = daily_challenge_rankings.user_id
      WHERE (CAST(user_rank AS FLOAT) / CAST(total_user_count AS FLOAT)) <= ?
      GROUP BY user_id
    )
    SELECT CAST(user_id AS UNSIGNED) as user_id, username, CAST(top_percent_count AS UNSIGNED) AS top_percent_count
    FROM top_counts
    LEFT JOIN users ON users.osu_id = top_counts.user_id
    ORDER BY top_percent_count DESC, top_counts.user_total_score_sum DESC;"#,
    percent,
  )
  .fetch_all(db_pool())
  .await
  .map_err(|err| {
    error!("Failed to load user top {percent}% rankings from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load user top {percent}% rankings from DB".to_owned(),
    }
  })?;
  let entries: Vec<DailyChallengePercentRankingEntry> = entries
    .into_iter()
    .enumerate()
    .map(|(rank, row)| DailyChallengePercentRankingEntry {
      user_id: row.user_id,
      username: row.username.unwrap_or_else(|| "Unknown".to_owned()),
      rank: rank as u64 + 1,
      top_percent_count: row.top_percent_count as usize,
    })
    .collect();

  let mut rankings_by_user_id = FxHashMap::default();
  let mut count_by_user_id = FxHashMap::default();
  for entry in &entries {
    rankings_by_user_id.insert(entry.user_id as usize, entry.rank as usize);
    count_by_user_id.insert(entry.user_id as usize, entry.top_percent_count);
  }

  Ok(UserPercentRankings {
    rank_by_user_id: rankings_by_user_id,
    count_by_user_id,
    rankings: entries,
  })
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct Histogram {
  min: f32,
  max: f32,
  buckets: Vec<usize>,
}

#[derive(Clone, Serialize)]
pub struct DailyChallengeStatsForDay {
  pub descriptor: DbDailyChallengeDescriptor,
  pub total_scores: usize,
  pub histogram: Histogram,
}

#[derive(Clone, Serialize)]
pub(crate) struct DailyChallengeTotalScoreRankingEntry {
  user_id: u64,
  username: String,
  rank: u64,
  total_score: u64,
}

pub(crate) struct UserTotalScoreRankings {
  rank_by_user_id: FxHashMap<usize, usize>,
  rankings: Vec<DailyChallengeTotalScoreRankingEntry>,
}

#[derive(Clone, Serialize)]
pub(crate) struct DailyChallengePercentRankingEntry {
  pub user_id: u64,
  pub username: String,
  pub rank: u64,
  pub top_percent_count: usize,
}

pub(crate) struct UserPercentRankings {
  rank_by_user_id: FxHashMap<usize, usize>,
  count_by_user_id: FxHashMap<usize, usize>,
  rankings: Vec<DailyChallengePercentRankingEntry>,
}

static DAILY_CHALLENGE_STATS: OnceCell<ArcSwap<FxHashMap<usize, DailyChallengeStatsForDay>>> =
  OnceCell::const_new();
static USER_TOTAL_SCORE_RANKINGS: OnceCell<ArcSwap<UserTotalScoreRankings>> = OnceCell::const_new();
static USER_TOP_1_PERCENT_RANKINGS: OnceCell<ArcSwap<UserPercentRankings>> = OnceCell::const_new();
static USER_TOP_10_PERCENT_RANKINGS: OnceCell<ArcSwap<UserPercentRankings>> = OnceCell::const_new();
static USER_TOP_50_PERCENT_RANKINGS: OnceCell<ArcSwap<UserPercentRankings>> = OnceCell::const_new();
static GLOBAL_DAILY_CHALLENGE_STATS: OnceCell<ArcSwap<GlobalDailyChallengeStats>> =
  OnceCell::const_new();

async fn get_daily_challenge_stats(
) -> Result<&'static ArcSwap<FxHashMap<usize, DailyChallengeStatsForDay>>, APIError> {
  DAILY_CHALLENGE_STATS
    .get_or_try_init(|| async {
      info!("Fetching daily challenge stats from DB...");
      let stats = load_daily_challenge_stats().await.map_err(|err| {
        error!("Failed to load daily challenge stats from DB: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to load daily challenge stats from DB".to_owned(),
        }
      })?;
      info!("Fetched {} daily challenge stats from DB", stats.len());
      Ok(ArcSwap::new(Arc::new(stats)))
    })
    .await
}

async fn get_user_total_score_rankings(
  fetch_missing_usernames: bool,
) -> Result<&'static ArcSwap<UserTotalScoreRankings>, APIError> {
  USER_TOTAL_SCORE_RANKINGS
    .get_or_try_init(|| async {
      info!("Fetching user total score rankings from DB...");
      let rankings = load_user_total_score_rankings(fetch_missing_usernames).await?;
      info!(
        "Fetched {} user total score rankings from DB",
        rankings.rank_by_user_id.len()
      );
      Ok(ArcSwap::new(Arc::new(rankings)))
    })
    .await
}

async fn get_user_top_1_percent_rankings() -> Result<&'static ArcSwap<UserPercentRankings>, APIError>
{
  USER_TOP_1_PERCENT_RANKINGS
    .get_or_try_init(|| async {
      info!("Fetching user top 1% rankings from DB...");
      let rankings = load_user_top_n_percent_rankings(0.01).await?;
      info!(
        "Fetched {} user top 1% rankings from DB",
        rankings.rank_by_user_id.len()
      );
      Ok(ArcSwap::new(Arc::new(rankings)))
    })
    .await
}

async fn get_user_top_10_percent_rankings(
) -> Result<&'static ArcSwap<UserPercentRankings>, APIError> {
  USER_TOP_10_PERCENT_RANKINGS
    .get_or_try_init(|| async {
      info!("Fetching user top 10% rankings from DB...");
      let rankings = load_user_top_n_percent_rankings(0.1).await?;
      info!(
        "Fetched {} user top 10% rankings from DB",
        rankings.rank_by_user_id.len()
      );
      Ok(ArcSwap::new(Arc::new(rankings)))
    })
    .await
}

async fn get_user_top_50_percent_rankings(
) -> Result<&'static ArcSwap<UserPercentRankings>, APIError> {
  USER_TOP_50_PERCENT_RANKINGS
    .get_or_try_init(|| async {
      info!("Fetching user top 50% rankings from DB...");
      let rankings = load_user_top_n_percent_rankings(0.5).await?;
      info!(
        "Fetched {} user top 50% rankings from DB",
        rankings.rank_by_user_id.len()
      );
      Ok(ArcSwap::new(Arc::new(rankings)))
    })
    .await
}

async fn get_global_daily_challenge_stats(
) -> Result<&'static ArcSwap<GlobalDailyChallengeStats>, APIError> {
  GLOBAL_DAILY_CHALLENGE_STATS
    .get_or_try_init(|| async {
      info!("Fetching global daily challenge stats from DB...");
      let stats = load_global_daily_challenge_stats().await?;
      info!("Fetched global daily challenge stats from DB");
      Ok(ArcSwap::new(Arc::new(stats)))
    })
    .await
}

#[derive(Deserialize)]
pub(crate) struct DailyChallengeHistoryQueryParams {
  start_day_id: Option<usize>,
  end_day_id: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct UserDailyChallengeHistoryEntry {
  pub score: UserDailyChallengeScore,
  pub total_rankings: i64,
  pub percentile: f32,
}

pub(crate) async fn get_user_daily_challenge_history(
  Path(user_id): Path<usize>,
  Query(DailyChallengeHistoryQueryParams {
    start_day_id,
    end_day_id,
  }): Query<DailyChallengeHistoryQueryParams>,
) -> Result<Json<Vec<UserDailyChallengeHistoryEntry>>, APIError> {
  let res: Result<Vec<UserDailyChallengeScore>, _> = match (start_day_id, end_day_id) {
    (None, None) => {
      let query = sqlx::query_as!(
        UserDailyChallengeScore,
        "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, \
         ended_at, mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE \
         user_id = ? ORDER BY day_id ASC",
        user_id as i64
      );
      query.fetch_all(db_pool()).await
    },
    (start, end) => {
      let query = sqlx::query_as!(
        UserDailyChallengeScore,
        "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, \
         ended_at, mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE \
         user_id = ? AND day_id >= ? AND day_id <= ? ORDER BY day_id ASC",
        user_id as i64,
        start.unwrap_or(0) as i64,
        end.unwrap_or(99999999) as i64
      );
      query.fetch_all(db_pool()).await
    },
  };

  let scores = res.map_err(|err| {
    error!("Failed to load daily challenge stats from DB for user {user_id}: {err}",);
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge stats from DB".to_owned(),
    }
  })?;

  let stats = get_daily_challenge_stats().await?.load();

  let entries = scores
    .into_iter()
    .map(|score| {
      let total_rankings = stats
        .get(&(score.day_id as usize))
        .map(|stats| stats.total_scores)
        .unwrap_or_else(|| {
          error!("No stats found for daily challenge {}", score.day_id);
          0
        }) as i64;
      let percentile = (score.user_rank as f32 / total_rankings as f32) * 100.;

      UserDailyChallengeHistoryEntry {
        score,
        total_rankings,
        percentile,
      }
    })
    .collect();
  Ok(Json(entries))
}

pub(crate) async fn get_user_daily_challenge_for_day(
  Path((user_id, day_id)): Path<(usize, usize)>,
) -> Result<Json<Option<UserDailyChallengeScore>>, APIError> {
  let query = sqlx::query_as!(
    UserDailyChallengeScore,
    "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, ended_at, \
     mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE user_id = ? AND \
     day_id = ?",
    user_id as i64,
    day_id as i64
  );
  let score = query.fetch_optional(db_pool()).await.map_err(|err| {
    error!(
      "Failed to load daily challenge stats from DB for user {} on day {}: {err}",
      user_id, day_id
    );
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge stats from DB".to_owned(),
    }
  })?;

  Ok(Json(score))
}

#[derive(Default, Serialize)]
pub(crate) struct Streaks {
  cur_daily_streak: usize,
  cur_weekly_streak: usize,
  best_daily_streak: usize,
  best_weekly_streak: usize,
  cur_top_1_percent_streak: usize,
  best_top_1_percent_streak: usize,
  best_top_1_percent_streak_span: Option<(usize, usize)>,
  cur_top_10_percent_streak: usize,
  best_top_10_percent_streak: usize,
  best_top_10_percent_streak_span: Option<(usize, usize)>,
  cur_top_50_percent_streak: usize,
  best_top_50_percent_streak: usize,
  best_top_50_percent_streak_span: Option<(usize, usize)>,
}

#[derive(Default, Serialize)]
pub(crate) struct BestPlacement {
  day_id: usize,
  score: usize,
  rank: usize,
  pp: Option<f32>,
  total_rankings: usize,
  percentile: f32,
}

#[derive(Default, Serialize)]
pub(crate) struct TotalScoreStats {
  /// The sum of all scores for all daily challenges
  total_score_sum: usize,
  /// Rank (1 being the best) of the sum of all scores for all daily challenges compared to all
  /// other users
  total_score_rank: usize,
  total_score_percentile: f32,
}

#[derive(Default, Serialize)]
pub(crate) struct DailyChallengeUserStats {
  pub total_participation: usize,
  pub total_challenge_count: usize,
  pub total_score_stats: TotalScoreStats,
  pub score_distribution: Histogram,
  /// Histogram with range [0, 86400] and 50 buckets indicating the distribution of times of day
  /// that the user submitted their best daily challenge score.
  pub time_of_day_distribution: Histogram,
  pub streaks: Streaks,
  pub top_1_percent_count: usize,
  pub top_1_percent_rank: Option<usize>,
  pub top_10_percent_count: usize,
  pub top_10_percent_rank: Option<usize>,
  pub top_50_percent_count: usize,
  pub top_50_percent_rank: Option<usize>,
  pub best_placement_absolute: Option<BestPlacement>,
  pub best_placement_percentile: Option<BestPlacement>,
  pub best_placement_score: Option<BestPlacement>,
  pub best_placement_pp: Option<BestPlacement>,
  pub most_used_mods: Vec<(Option<Mod>, usize)>,
}

#[derive(sqlx::FromRow)]
struct MinimalUserDailyChallengeScore {
  day_id: i64,
  user_rank: i64,
  total_score: i64,
  ended_at: Option<DateTime<Utc>>,
  mods: Option<Vec<u8>>,
  pp: Option<f32>,
}

fn day_id_to_naive_date(day_id: usize) -> NaiveDate {
  let day_id_str = format!("{day_id}");
  NaiveDate::parse_from_str(
    &format!(
      "{}-{}-{}",
      &day_id_str[0..4],
      &day_id_str[4..6],
      &day_id_str[6..8]
    ),
    "%Y-%m-%d",
  )
  .unwrap()
}

fn start_of_week(date: NaiveDate) -> NaiveDate {
  // Start of week is Thursday, following osu!'s week streak calculation:
  // https://github.com/ppy/osu-web/blob/dcfce28dd8263e141c3efac53fc356a5c5a5ec7e/app/Models/DailyChallengeUserStats.php#L90
  let mut start = date;
  while start.weekday() != Weekday::Thu {
    start = start.pred_opt().unwrap();
  }
  start
}

async fn compute_streaks(
  scores: &[MinimalUserDailyChallengeScore],
  last_daily_challenge_day_id: usize,
) -> Result<Streaks, APIError> {
  if scores.is_empty() {
    return Ok(Streaks::default());
  }

  let mut cur_daily_streak = 1usize;
  let mut cur_top_1_percent_streak = 0usize;
  let mut cur_top_1_percent_streak_span: Option<(usize, usize)> = None;
  let mut cur_top_10_percent_streak = 0usize;
  let mut cur_top_10_percent_streak_span: Option<(usize, usize)> = None;
  let mut cur_top_50_percent_streak = 0usize;
  let mut cur_top_50_percent_streak_span: Option<(usize, usize)> = None;
  let mut best_daily_streak = 1usize;
  let mut best_top_1_percent_streak = 0usize;
  let mut best_top_1_percent_streak_span: Option<(usize, usize)> = None;
  let mut best_top_10_percent_streak = 0usize;
  let mut best_top_10_percent_streak_span: Option<(usize, usize)> = None;
  let mut best_top_50_percent_streak = 0usize;
  let mut best_top_50_percent_streak_span: Option<(usize, usize)> = None;

  let mut seen_week_ids = FxHashSet::default();
  seen_week_ids.insert(start_of_week(day_id_to_naive_date(
    scores[0].day_id as usize,
  )));

  let mut last_top_1_percent_day_id: Option<usize> = None;
  let mut last_top_10_percent_day_id: Option<usize> = None;
  let mut last_top_50_percent_day_id: Option<usize> = None;

  let stats = get_daily_challenge_stats().await?.load();
  for i in 1..scores.len() {
    let prev = &scores[i - 1];
    let prev_date = day_id_to_naive_date(prev.day_id as usize);
    let cur = &scores[i];
    let cur_date = day_id_to_naive_date(cur.day_id as usize);

    if prev_date.succ_opt().unwrap() == cur_date {
      cur_daily_streak += 1;
    } else {
      best_daily_streak = best_daily_streak.max(cur_daily_streak);
      cur_daily_streak = 1;
    }

    seen_week_ids.insert(start_of_week(cur_date));
  }

  for i in 0..scores.len() {
    let cur = &scores[i];
    let cur_date = day_id_to_naive_date(cur.day_id as usize);

    let total_rankings_for_day = stats
      .get(&(cur.day_id as usize))
      .map(|s| s.total_scores)
      .unwrap_or(0);
    let percentile = (cur.user_rank as f32 / total_rankings_for_day as f32) * 100.;
    let is_top_1_percent = percentile <= 1.;
    let is_top_10_percent = percentile <= 10.;
    let is_top_50_percent = percentile <= 50.;

    if is_top_1_percent {
      if let Some(last_day_id) = last_top_1_percent_day_id {
        let prev_date = day_id_to_naive_date(last_day_id);
        let is_successive = prev_date.succ_opt().unwrap() == cur_date;

        if is_successive {
          cur_top_1_percent_streak += 1;
          cur_top_1_percent_streak_span.as_mut().unwrap().1 = cur.day_id as usize;
        } else {
          cur_top_1_percent_streak = 1;
          cur_top_1_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
        }
      } else {
        cur_top_1_percent_streak = 1;
        cur_top_1_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
      }
      last_top_1_percent_day_id = Some(cur.day_id as usize);

      if cur_top_1_percent_streak > best_top_1_percent_streak {
        best_top_1_percent_streak = cur_top_1_percent_streak;
        best_top_1_percent_streak_span = cur_top_1_percent_streak_span;
      }
    } else {
      cur_top_1_percent_streak = 0;
    }

    if is_top_10_percent {
      if let Some(last_day_id) = last_top_10_percent_day_id {
        let prev_date = day_id_to_naive_date(last_day_id);
        let is_successive = prev_date.succ_opt().unwrap() == cur_date;

        if is_successive {
          cur_top_10_percent_streak += 1;
          cur_top_10_percent_streak_span.as_mut().unwrap().1 = cur.day_id as usize;
        } else {
          cur_top_10_percent_streak = 1;
          cur_top_10_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
        }
      } else {
        cur_top_10_percent_streak = 1;
        cur_top_10_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
      }
      last_top_10_percent_day_id = Some(cur.day_id as usize);

      if cur_top_10_percent_streak > best_top_10_percent_streak {
        best_top_10_percent_streak = cur_top_10_percent_streak;
        best_top_10_percent_streak_span = cur_top_10_percent_streak_span;
      }
    } else {
      cur_top_10_percent_streak = 0;
    }

    if is_top_50_percent {
      if let Some(last_day_id) = last_top_50_percent_day_id {
        let prev_date = day_id_to_naive_date(last_day_id);
        let is_successive = prev_date.succ_opt().unwrap() == cur_date;

        if is_successive {
          cur_top_50_percent_streak += 1;
          cur_top_50_percent_streak_span.as_mut().unwrap().1 = cur.day_id as usize;
        } else {
          cur_top_50_percent_streak = 1;
          cur_top_50_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
        }
      } else {
        cur_top_50_percent_streak = 1;
        cur_top_50_percent_streak_span = Some((cur.day_id as usize, cur.day_id as usize));
      }
      last_top_50_percent_day_id = Some(cur.day_id as usize);

      if cur_top_50_percent_streak > best_top_50_percent_streak {
        best_top_50_percent_streak = cur_top_50_percent_streak;
        best_top_50_percent_streak_span = cur_top_50_percent_streak_span;
      }
    } else {
      cur_top_50_percent_streak = 0;
    }
  }

  let mut all_week_ids: Vec<NaiveDate> = seen_week_ids.into_iter().collect();
  all_week_ids.sort_unstable();

  let mut cur_weekly_streak = 1;
  let mut best_weekly_streak = 1;

  for i in 1..all_week_ids.len() {
    let prev_week = &all_week_ids[i - 1];
    let cur_week = &all_week_ids[i];

    if *cur_week == prev_week.checked_add_days(Days::new(7)).unwrap() {
      cur_weekly_streak += 1;
    } else {
      best_weekly_streak = best_weekly_streak.max(cur_weekly_streak);
      cur_weekly_streak = 1;
    }
  }

  let last_daily_challenge_date = day_id_to_naive_date(last_daily_challenge_day_id);
  if last_daily_challenge_day_id != scores.last().unwrap().day_id as usize {
    cur_daily_streak = 0;
  }

  let last_challenge_week = start_of_week(last_daily_challenge_date);
  if last_challenge_week != all_week_ids.last().copied().unwrap() {
    cur_weekly_streak = 0;
  }

  Ok(Streaks {
    cur_daily_streak,
    cur_weekly_streak,
    best_daily_streak: best_daily_streak.max(cur_daily_streak),
    best_weekly_streak: best_weekly_streak.max(cur_weekly_streak),
    cur_top_1_percent_streak,
    best_top_1_percent_streak,
    best_top_1_percent_streak_span,
    cur_top_10_percent_streak,
    best_top_10_percent_streak,
    best_top_10_percent_streak_span,
    cur_top_50_percent_streak,
    best_top_50_percent_streak,
    best_top_50_percent_streak_span,
  })
}

pub(crate) async fn get_user_daily_challenge_stats(
  Path(user_id): Path<usize>,
) -> Result<Json<DailyChallengeUserStats>, APIError> {
  let scores = sqlx::query_as!(
    MinimalUserDailyChallengeScore,
    "SELECT day_id, user_rank, total_score, ended_at, mods, pp FROM daily_challenge_rankings \
     WHERE user_id = ? ORDER BY day_id ASC",
    user_id as i64
  )
  .fetch_all(db_pool())
  .await
  .map_err(|err| {
    error!(
      "Failed to load daily challenge stats from DB for user {}: {err}",
      user_id
    );
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge stats from DB".to_owned(),
    }
  })?;

  let stats = get_daily_challenge_stats().await?.load();

  if scores.is_empty() {
    return Ok(Json(DailyChallengeUserStats {
      total_challenge_count: stats.len(),
      ..Default::default()
    }));
  }

  let mut total_score_sum = 0;
  let (total_score_rank, total_score_percentile) = {
    let stats = get_user_total_score_rankings(false).await?.load();
    let total_score_rank = stats.rank_by_user_id.get(&user_id).copied().unwrap_or(0);
    let total_user_count = stats.rank_by_user_id.len();
    (
      total_score_rank,
      (total_score_rank as f32) / (total_user_count as f32) * 100.,
    )
  };

  let (top_1_percent_rank, top_1_percent_count) = {
    let stats = get_user_top_1_percent_rankings().await?.load();
    let top_1_percent_rank = stats.rank_by_user_id.get(&user_id).copied();
    let top_1_percent_count = stats.count_by_user_id.get(&user_id).copied().unwrap_or(0);
    (top_1_percent_rank, top_1_percent_count)
  };
  let (top_10_percent_rank, top_10_percent_count) = {
    let stats = get_user_top_10_percent_rankings().await?.load();
    let top_10_percent_rank = stats.rank_by_user_id.get(&user_id).copied();
    let top_10_percent_count = stats.count_by_user_id.get(&user_id).copied().unwrap_or(0);
    (top_10_percent_rank, top_10_percent_count)
  };
  let (top_50_percent_rank, top_50_percent_count) = {
    let stats = get_user_top_50_percent_rankings().await?.load();
    let top_50_percent_rank = stats.rank_by_user_id.get(&user_id).copied();
    let top_50_percent_count = stats.count_by_user_id.get(&user_id).copied().unwrap_or(0);
    (top_50_percent_rank, top_50_percent_count)
  };

  let mut best_placement_absolute: BestPlacement = BestPlacement::default();
  let mut best_placement_percentile: BestPlacement = BestPlacement::default();
  let mut best_placement_score: BestPlacement = BestPlacement::default();
  let mut best_placement_pp: BestPlacement = BestPlacement::default();

  let score_histogram_bucket_count = if scores.len() < 10 {
    10
  } else if scores.len() < 50 {
    25
  } else {
    50
  };
  let mut score_histogram = vec![0; score_histogram_bucket_count];
  let score_histogram_min = 0usize;
  let mut score_histogram_max = scores
    .iter()
    .map(|s| s.total_score as usize)
    .max()
    .unwrap_or(1_200_000)
    .max(1_200_000);
  if score_histogram_max > 1_500_000 {
    score_histogram_max = 1_600_000;
  } else if score_histogram_max > 1_400_000 {
    score_histogram_max = 1_500_000;
  } else if score_histogram_max > 1_300_000 {
    score_histogram_max = 1_400_000;
  } else if score_histogram_max > 1_200_000 {
    score_histogram_max = 1_300_000;
  }
  let score_histogram_bucket_size =
    ((score_histogram_max - score_histogram_min) / score_histogram_bucket_count).max(1);

  let time_of_day_histogram_bucket_count = 24;
  let mut time_of_day_histogram = vec![0; time_of_day_histogram_bucket_count];
  let time_of_day_histogram_min = 0usize;
  let time_of_day_histogram_max = 86400usize;
  let time_of_day_histogram_bucket_size =
    (time_of_day_histogram_max - time_of_day_histogram_min) / time_of_day_histogram_bucket_count;

  let last_daily_challenge_day_id = stats.keys().max().copied().unwrap();
  let streaks = compute_streaks(&scores, last_daily_challenge_day_id).await?;
  let mut most_used_mods = FxHashMap::default();

  for score in &scores {
    total_score_sum += score.total_score as usize;

    let total_rankings = stats
      .get(&(score.day_id as usize))
      .map(|s| s.total_scores)
      .unwrap_or(0);

    let percentile = (score.user_rank as f32 / total_rankings as f32) * 100.;

    if best_placement_absolute.rank == 0
      || (score.user_rank as usize) < best_placement_absolute.rank
    {
      best_placement_absolute = BestPlacement {
        day_id: score.day_id as usize,
        score: score.total_score as usize,
        rank: score.user_rank as usize,
        total_rankings,
        percentile,
        pp: score.pp,
      };
    }

    if best_placement_percentile.rank == 0 || percentile < best_placement_percentile.percentile {
      best_placement_percentile = BestPlacement {
        day_id: score.day_id as usize,
        score: score.total_score as usize,
        rank: score.user_rank as usize,
        total_rankings,
        percentile,
        pp: score.pp,
      };
    }

    if best_placement_score.score == 0 || (score.total_score as usize) > best_placement_score.score
    {
      best_placement_score = BestPlacement {
        day_id: score.day_id as usize,
        score: score.total_score as usize,
        rank: score.user_rank as usize,
        total_rankings,
        percentile,
        pp: score.pp,
      };
    }

    if (best_placement_pp.pp.is_none() && score.pp.is_some())
      || (score.pp.is_some() && score.pp.unwrap() > best_placement_pp.pp.unwrap_or(0.))
    {
      best_placement_pp = BestPlacement {
        day_id: score.day_id as usize,
        score: score.total_score as usize,
        rank: score.user_rank as usize,
        total_rankings,
        percentile,
        pp: score.pp,
      };
    }

    let score_bucket_index = ((score.total_score as usize - score_histogram_min)
      .min(score_histogram_max)
      / score_histogram_bucket_size)
      .min(score_histogram.len() - 1);
    score_histogram[score_bucket_index] += 1;

    if let Some(ended_at) = score.ended_at {
      let time_of_day = Timelike::num_seconds_from_midnight(&ended_at.time()) as usize;
      let time_of_day_bucket_index = ((time_of_day - time_of_day_histogram_min)
        .min(time_of_day_histogram_max)
        / time_of_day_histogram_bucket_size)
        .min(time_of_day_histogram.len() - 1);
      time_of_day_histogram[time_of_day_bucket_index] += 1;
    }

    match &score.mods {
      Some(mods) => match serde_json::from_slice::<Vec<Mod>>(mods) {
        Ok(mods) if mods.is_empty() => {
          most_used_mods
            .entry(None)
            .or_insert((0, score.day_id as usize))
            .0 += 1;
        },
        Ok(mods) =>
          for m in mods {
            let entry = most_used_mods
              .entry(Some(m))
              .or_insert((0, score.day_id as usize));
            entry.0 += 1;
            entry.1 = entry.1.max(score.day_id as usize);
          },
        Err(_) => {
          error!(
            "Failed to parse mods for daily challenge score for user={user_id} day_id={}; found: \
             {}",
            score.day_id,
            String::from_utf8_lossy(mods)
          );
        },
      },
      None => (),
    }
  }

  let mut most_used_mods: Vec<_> = most_used_mods.into_iter().collect();
  most_used_mods.sort_unstable_by_key(|(_, (count, most_recent_day_id))| {
    Reverse((*count, *most_recent_day_id))
  });
  let most_used_mods = most_used_mods
    .into_iter()
    .map(|(m, (count, _))| (m, count))
    .collect();

  Ok(Json(DailyChallengeUserStats {
    total_participation: scores.len(),
    total_challenge_count: stats.len(),
    total_score_stats: TotalScoreStats {
      total_score_sum,
      total_score_rank,
      total_score_percentile,
    },
    score_distribution: Histogram {
      min: score_histogram_min as f32,
      max: score_histogram_max as f32,
      buckets: score_histogram,
    },
    time_of_day_distribution: Histogram {
      min: time_of_day_histogram_min as f32,
      max: time_of_day_histogram_max as f32,
      buckets: time_of_day_histogram,
    },
    streaks,
    top_1_percent_count,
    top_1_percent_rank,
    top_10_percent_count,
    top_50_percent_count,
    top_10_percent_rank,
    top_50_percent_rank,
    best_placement_absolute: if best_placement_absolute.rank == 0 {
      None
    } else {
      Some(best_placement_absolute)
    },
    best_placement_percentile: if best_placement_percentile.rank == 0 {
      None
    } else {
      Some(best_placement_percentile)
    },
    best_placement_score: if best_placement_score.score == 0 {
      None
    } else {
      Some(best_placement_score)
    },
    best_placement_pp: if best_placement_pp.pp.is_none() {
      None
    } else {
      Some(best_placement_pp)
    },
    most_used_mods,
  }))
}

pub(crate) async fn get_daily_challenge_stats_for_day(
  Path(day_id): Path<usize>,
) -> Result<Json<DailyChallengeStatsForDay>, APIError> {
  let stats = get_daily_challenge_stats().await?.load();
  let stats = stats.get(&day_id).ok_or_else(|| {
    error!("No stats found for daily challenge {}", day_id);
    APIError {
      status: StatusCode::NOT_FOUND,
      message: "No stats found for daily challenge".to_owned(),
    }
  })?;
  Ok(Json(stats.clone()))
}

pub(crate) async fn get_daily_challenge_rankings_for_day(
  Path(day_id): Path<usize>,
  Query(LoadUserTotalScoreRankingsQueryParams {
    page,
    fetch_missing_usernames: _,
  }): Query<LoadUserTotalScoreRankingsQueryParams>,
) -> Result<Json<Vec<DailyChallengeTotalScoreRankingEntry>>, APIError> {
  let page_size = 50;
  let start = (page.unwrap_or(1).max(1) - 1) * page_size;
  let query = sqlx::query!(
    r#"
    SELECT
      CAST(user_id as UNSIGNED) AS user_id,
      IFNULL(username, 'Unknown') as username,
      CAST(total_score as UNSIGNED) AS total_score
    FROM daily_challenge_rankings
    LEFT JOIN users
      ON users.osu_id = daily_challenge_rankings.user_id
    WHERE day_id = ?
    ORDER BY total_score DESC, ended_at ASC
    LIMIT ?, ?
    "#,
    day_id as i64,
    start as i64,
    page_size as i64
  );
  let rankings = query.fetch_all(db_pool()).await.map_err(|err| {
    error!("Failed to load daily challenge rankings from DB for day {day_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge rankings from DB".to_owned(),
    }
  })?;
  let rankings: Vec<DailyChallengeTotalScoreRankingEntry> = rankings
    .into_iter()
    .enumerate()
    .map(|(i, row)| DailyChallengeTotalScoreRankingEntry {
      user_id: row.user_id,
      username: row.username,
      rank: start as u64 + i as u64 + 1,
      total_score: row.total_score,
    })
    .collect();
  Ok(Json(rankings))
}

#[derive(Serialize)]
pub(crate) struct GetDailyChallengeTotalScoreRankingsResponse {
  pub rankings: Vec<DailyChallengeTotalScoreRankingEntry>,
  pub total_rankings: usize,
}

pub(crate) async fn get_daily_challenge_total_score_rankings(
  Query(LoadUserTotalScoreRankingsQueryParams {
    page,
    fetch_missing_usernames: _,
  }): Query<LoadUserTotalScoreRankingsQueryParams>,
) -> Result<Json<GetDailyChallengeTotalScoreRankingsResponse>, APIError> {
  let rankings = get_user_total_score_rankings(false).await?.load();
  let page_size = 50;
  let start = page.unwrap_or(1).max(1) * page_size - page_size;
  let end = start + page_size;
  let page_rankings = rankings
    .rankings
    .get(start..(end.min(rankings.rankings.len() - 1)))
    .unwrap_or_default();
  Ok(Json(GetDailyChallengeTotalScoreRankingsResponse {
    rankings: page_rankings.to_owned(),
    total_rankings: rankings.rankings.len(),
  }))
}

#[derive(Deserialize)]
pub(crate) struct GetDailyChallengePercentRankingsQueryParams {
  page: Option<usize>,
}

#[derive(Serialize)]
pub(crate) struct GetDailyChallengePercentRankingsResponse {
  pub rankings: Vec<DailyChallengePercentRankingEntry>,
  pub total_rankings: usize,
}

async fn get_daily_challenge_percent_rankings(
  percent: usize,
  page: Option<usize>,
) -> Result<Json<GetDailyChallengePercentRankingsResponse>, APIError> {
  let page_size = 50;
  let start = page.unwrap_or(1).max(1) * page_size - page_size;
  let end = start + page_size;
  let rankings = match percent {
    50 => get_user_top_50_percent_rankings().await?.load(),
    10 => get_user_top_10_percent_rankings().await?.load(),
    1 => get_user_top_1_percent_rankings().await?.load(),
    _ => {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: "Invalid percent".to_owned(),
      });
    },
  };
  let page_rankings = rankings
    .rankings
    .get(start..(end.min(rankings.rankings.len() - 1)))
    .unwrap_or_default();
  Ok(Json(GetDailyChallengePercentRankingsResponse {
    rankings: page_rankings.to_owned(),
    total_rankings: rankings.rankings.len(),
  }))
}

pub(crate) async fn get_daily_challenge_top_50_percent_rankings(
  Query(GetDailyChallengePercentRankingsQueryParams { page }): Query<
    GetDailyChallengePercentRankingsQueryParams,
  >,
) -> Result<Json<GetDailyChallengePercentRankingsResponse>, APIError> {
  get_daily_challenge_percent_rankings(50, page).await
}

pub(crate) async fn get_daily_challenge_top_10_percent_rankings(
  Query(GetDailyChallengePercentRankingsQueryParams { page }): Query<
    GetDailyChallengePercentRankingsQueryParams,
  >,
) -> Result<Json<GetDailyChallengePercentRankingsResponse>, APIError> {
  get_daily_challenge_percent_rankings(10, page).await
}

pub(crate) async fn get_daily_challenge_top_1_percent_rankings(
  Query(GetDailyChallengePercentRankingsQueryParams { page }): Query<
    GetDailyChallengePercentRankingsQueryParams,
  >,
) -> Result<Json<GetDailyChallengePercentRankingsResponse>, APIError> {
  get_daily_challenge_percent_rankings(1, page).await
}

pub(crate) async fn get_daily_challenge_global_stats(
) -> Result<Json<GlobalDailyChallengeStats>, APIError> {
  let stats = get_global_daily_challenge_stats().await?.load();
  Ok(Json((**stats).clone()))
}

pub(crate) async fn get_latest_daily_challenge_day_id() -> Result<Json<usize>, APIError> {
  let query = sqlx::query_scalar!("SELECT MAX(day_id) FROM daily_challenge_metadata");
  let day_id = query
    .fetch_one(db_pool())
    .await
    .map_err(|err| {
      error!("Failed to fetch latest daily challenge day ID from DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch latest daily challenge day ID from DB".to_owned(),
      }
    })?
    .expect("looks like the database has been emptied");
  Ok(Json(day_id as usize))
}
