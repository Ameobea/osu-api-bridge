use std::cmp::Reverse;

use chrono::DateTime;
use fxhash::{FxHashMap, FxHashSet};
use serde::Serialize;
use sqlx::{Executor, MySql, QueryBuilder};
use tokio::sync::OnceCell;

use crate::{
  db::db_pool,
  osu_api::{
    self,
    daily_challenge::{DailyChallengeScore, NewDailyChallengeDescriptor},
  },
  util::serialize_json_bytes_opt,
};

use super::*;

async fn store_daily_challenge_metadata(
  metadata: &[NewDailyChallengeDescriptor],
) -> sqlx::Result<()> {
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

  let pool = db_pool();
  pool.execute(query).await?;
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
#[derive(Serialize, sqlx::FromRow)]
pub(crate) struct UserDailyChallengeScore {
  day_id: i64,
  user_id: i64,
  score_id: i64,
  pp: Option<f32>,
  rank: Option<String>,
  #[serde(serialize_with = "serialize_json_bytes_opt")]
  statistics: Option<Vec<u8>>,
  total_score: i64,
  started_at: Option<DateTime<chrono::Utc>>,
  ended_at: Option<DateTime<chrono::Utc>>,
  #[serde(serialize_with = "serialize_json_bytes_opt")]
  mods: Option<Vec<u8>>,
  max_combo: i64,
  accuracy: f32,
  user_rank: i64,
}

async fn store_daily_challenge_scores(
  day_id: usize,
  mut scores: Vec<DailyChallengeScore>,
) -> sqlx::Result<()> {
  let pool = db_pool();
  let mut txn = pool.begin().await.unwrap();

  scores.sort_unstable_by_key(|score| Reverse((score.total_score, score.ended_at)));

  const CHUNK_SIZE: usize = 50;
  for (chunk_ix, scores) in scores.chunks(CHUNK_SIZE).enumerate() {
    let mut qb: QueryBuilder<'_, MySql> = QueryBuilder::new(
      "INSERT INTO daily_challenge_rankings (day_id, user_id, score_id, pp, rank, statistics, \
       total_score, started_at, ended_at, mods, max_combo, accuracy, user_rank) ",
    );
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
       VALUES(statistics), total_score = VALUES(total_score), started_at = VALUES(started_at), \
       mods = VALUES(mods), max_combo = VALUES(max_combo), accuracy = VALUES(accuracy)",
    );
    let query = qb.build();
    txn.execute(query).await?;
  }

  txn.commit().await
}

pub(super) async fn backfill_daily_challenges(admin_api_token: String) -> Result<(), APIError> {
  if admin_api_token != SETTINGS.load().daily_challenge.admin_token {
    if admin_api_token.is_empty() {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: "Missing admin API token in request body".to_string(),
      });
    }
    return Err(APIError {
      status: StatusCode::UNAUTHORIZED,
      message: "Invalid admin API token in request body".to_string(),
    });
  }

  let mut all_daily_challenge_ids =
    osu_api::daily_challenge::get_daily_challenge_descriptors(false).await?;

  let mut conn = db_pool().acquire().await.map_err(|err| {
    error!("Failed to acquire DB connection for daily challenge backfill: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to acquire DB connection for daily challenge backfill".to_string(),
    }
  })?;
  let already_collected_day_ids: FxHashSet<usize> =
    sqlx::query_scalar!("SELECT day_id FROM daily_challenge_metadata;")
      .fetch_all(&mut *conn)
      .await
      .map_err(|err| {
        error!("Failed to fetch already collected daily challenge IDs from DB: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to fetch already collected daily challenge IDs from DB".to_string(),
        }
      })?
      .into_iter()
      .map(|id| id as usize)
      .collect();
  drop(conn);

  all_daily_challenge_ids.retain(|ids| !already_collected_day_ids.contains(&ids.day_id));
  store_daily_challenge_metadata(&all_daily_challenge_ids)
    .await
    .map_err(|err| {
      error!("Failed to store daily challenge metadata in DB: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to store daily challenge metadata in DB".to_string(),
      }
    })?;

  if all_daily_challenge_ids.is_empty() {
    return Ok(());
  }

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

    match store_daily_challenge_scores(ids.day_id, scores).await {
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

  // Update the daily challenge stats cache
  match load_daily_challenge_stats().await {
    Ok(new_stats) => get_daily_challenge_stats().await.store({
      info!("Refreshing daily challenge stats cache...");
      Arc::new(new_stats)
    }),
    Err(err) => {
      error!("Failed to refresh daily challenge stats cache: {err}");
    },
  }

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

        ScoreHistogram { min, max, buckets }
      })
      .unwrap_or_else(|| ScoreHistogram {
        min: 0,
        max: 1_000_000,
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
pub(crate) struct ScoreHistogram {
  min: usize,
  max: usize,
  buckets: Vec<usize>,
}

#[derive(Clone, Serialize)]
pub struct DailyChallengeStatsForDay {
  pub descriptor: DbDailyChallengeDescriptor,
  pub total_scores: usize,
  pub histogram: ScoreHistogram,
}

static DAILY_CHALLENGE_STATS: OnceCell<ArcSwap<FxHashMap<usize, DailyChallengeStatsForDay>>> =
  OnceCell::const_new();

async fn get_daily_challenge_stats() -> &'static ArcSwap<FxHashMap<usize, DailyChallengeStatsForDay>>
{
  DAILY_CHALLENGE_STATS
    .get_or_init(|| async {
      info!("Fetching daily challenge stats from DB...");
      let stats = load_daily_challenge_stats().await.unwrap();
      info!("Fetched {} daily challenge stats from DB", stats.len());
      ArcSwap::new(Arc::new(stats))
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
  let pool = db_pool();
  let mut conn = pool.acquire().await.map_err(|err| {
    error!("Failed to acquire DB connection for daily challenge stats: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to acquire DB connection for daily challenge stats".to_string(),
    }
  })?;

  let res: Result<Vec<UserDailyChallengeScore>, _> = match (start_day_id, end_day_id) {
    (None, None) => {
      let query = sqlx::query_as!(
        UserDailyChallengeScore,
        "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, \
         ended_at, mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE \
         user_id = ? ORDER BY day_id ASC",
        user_id as i64
      );
      query.fetch_all(&mut *conn).await
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
      query.fetch_all(&mut *conn).await
    },
  };

  let scores = res.map_err(|err| {
    error!(
      "Failed to load daily challenge stats from DB for user {}: {err}",
      user_id
    );
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge stats from DB".to_string(),
    }
  })?;

  let stats = get_daily_challenge_stats().await.load();

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
  let pool = db_pool();
  let mut conn = pool.acquire().await.map_err(|err| {
    error!("Failed to acquire DB connection for daily challenge stats: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to acquire DB connection for daily challenge stats".to_string(),
    }
  })?;

  let query = sqlx::query_as!(
    UserDailyChallengeScore,
    "SELECT day_id, user_id, score_id, pp, rank, statistics, total_score, started_at, ended_at, \
     mods, max_combo, accuracy, user_rank FROM daily_challenge_rankings WHERE user_id = ? AND \
     day_id = ?",
    user_id as i64,
    day_id as i64
  );
  let score = query.fetch_optional(&mut *conn).await.map_err(|err| {
    error!(
      "Failed to load daily challenge stats from DB for user {} on day {}: {err}",
      user_id, day_id
    );
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to load daily challenge stats from DB".to_string(),
    }
  })?;

  Ok(Json(score))
}

pub(crate) async fn get_daily_challenge_stats_for_day(
  Path(day_id): Path<usize>,
) -> Result<Json<DailyChallengeStatsForDay>, APIError> {
  let stats = get_daily_challenge_stats().await.load();
  let stats = stats.get(&day_id).ok_or_else(|| {
    error!("No stats found for daily challenge {}", day_id);
    APIError {
      status: StatusCode::NOT_FOUND,
      message: "No stats found for daily challenge".to_string(),
    }
  })?;
  Ok(Json(stats.clone()))
}
