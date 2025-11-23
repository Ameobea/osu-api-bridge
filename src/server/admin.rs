use std::time::Duration;

use axum::extract::Query;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::{db::db_pool, osu_api::fetch_username};

use super::{APIError, SETTINGS};

pub(crate) fn validate_admin_api_token(admin_api_token: &str) -> Result<(), APIError> {
  if admin_api_token != SETTINGS.get().unwrap().daily_challenge.admin_token {
    if admin_api_token.is_empty() {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: "Missing admin API token in request body".to_owned(),
      });
    }
    return Err(APIError {
      status: StatusCode::UNAUTHORIZED,
      message: "Invalid admin API token in request body".to_owned(),
    });
  }

  Ok(())
}

/// For users that no longer show up in data from osu! API, they're likely banned.
///
/// This function copies all their hiscores and updates from `hiscore_updates` ->
/// `hiscore_updates_deleted` -> `updates` to `updates_deleted`, then finally delete the user from
/// `users`.
async fn archive_and_delete_user(user_id: i64) -> Result<(), APIError> {
  let pool = db_pool();
  let mut txn = pool.begin().await.map_err(|err| {
    error!("Error starting transaction: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error starting transaction".to_owned(),
    }
  })?;

  sqlx::query(
    "INSERT INTO hiscore_updates_deleted (user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic) SELECT user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic FROM hiscore_updates WHERE user = ?;",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error copying hiscore updates to hiscore_updates_deleted: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error copying hiscore updates to hiscore_updates_deleted".to_owned(),
    }
  })?;

  sqlx::query(
    "INSERT INTO updates_deleted (user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode) SELECT user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode FROM updates WHERE user = ?;",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error copying updates to updates_deleted: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error copying updates to updates_deleted".to_owned(),
    }
  })?;

  sqlx::query("DELETE FROM hiscore_updates WHERE user = ?;")
    .bind(user_id)
    .execute(&mut *txn)
    .await
    .map_err(|err| {
      error!("Error deleting user hiscores: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Error deleting user hiscores".to_owned(),
      }
    })?;

  sqlx::query("DELETE FROM updates WHERE user = ?;")
    .bind(user_id)
    .execute(&mut *txn)
    .await
    .map_err(|err| {
      error!("Error deleting user updates: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Error deleting user updates".to_owned(),
      }
    })?;

  sqlx::query("DELETE FROM users WHERE osu_id = ?;")
    .bind(user_id)
    .execute(&mut *txn)
    .await
    .map_err(|err| {
      error!("Error deleting user: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Error deleting user".to_owned(),
      }
    })?;

  txn.commit().await.map_err(|err| {
    error!("Error committing transaction to delete user hiscores + updates: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error committing transaction to delete user hiscores + updates".to_owned(),
    }
  })?;

  Ok(())
}

pub(super) async fn verify_best_plays(admin_api_token: String) -> Result<(), APIError> {
  validate_admin_api_token(&admin_api_token)?;

  info!("Verifying top plays...");
  let pool = db_pool();
  let top_plays: Vec<i64> = sqlx::query_scalar(
    "SELECT DISTINCT user FROM hiscore_updates WHERE mode=0 ORDER BY pp DESC limit 200;",
  )
  .fetch_all(pool)
  .await
  .map_err(|err| {
    error!("Error fetching top plays: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error fetching top plays".to_owned(),
    }
  })?;

  for (i, user_id) in top_plays.into_iter().enumerate() {
    let is_first_user = i == 0;
    if !is_first_user {
      tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // check to see if the user is still around
    match fetch_username(user_id as u64).await {
      Ok(Some(_username)) => continue,
      Ok(None) => {
        warn!("User ID {user_id} no longer exists in osu! API; deleting their hiscores + updates");
      },
      Err(err) => {
        error!("Error fetching username from osu! API for user ID {user_id}; err={err:?}");
        continue;
      },
    }

    if let Err(err) = archive_and_delete_user(user_id).await {
      error!("Error archiving and deleting user ID {user_id}: {err:?}");
    } else {
      info!("Successfully archived and deleted data for user id={user_id}");
    }
  }

  info!("Done verifying top plays");
  Ok(())
}

#[derive(Deserialize)]
pub(super) struct UndeleteUserQueryParams {
  user_id: i64,
}

pub(super) async fn maybe_undelete_user(
  Query(UndeleteUserQueryParams { user_id }): Query<UndeleteUserQueryParams>,
  admin_api_token: String,
) -> Result<(), APIError> {
  validate_admin_api_token(&admin_api_token)?;

  let pool = db_pool();
  let mut txn = pool.begin().await.map_err(|err| {
    error!("Error starting transaction: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error starting transaction".to_owned(),
    }
  })?;

  match fetch_username(user_id as u64).await {
    Ok(Some(_username)) => {},
    Ok(None) => {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: "Cannot undelete user that does not exist in osu! API".to_owned(),
      });
    },
    Err(err) => {
      error!("Error fetching username from osu! API for user ID {user_id}; err={err:?}");
      return Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Error fetching username from osu! API".to_owned(),
      });
    },
  };

  sqlx::query(
    "INSERT IGNORE INTO hiscore_updates (user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic) SELECT user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic FROM hiscore_updates_deleted WHERE user = ?;",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error restoring hiscore updates from hiscore_updates_deleted: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error restoring hiscore updates from hiscore_updates_deleted".to_owned(),
    }
  })?;

  sqlx::query(
    "INSERT IGNORE INTO updates (user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode) SELECT user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode FROM updates_deleted WHERE user = ?;",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error restoring updates from updates_deleted: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error restoring updates from updates_deleted".to_owned(),
    }
  })?;

  txn.commit().await.map_err(|err| {
    error!("Error committing transaction to restore user hiscores + updates: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error committing transaction to restore user hiscores + updates".to_owned(),
    }
  })?;
  Ok(())
}

#[derive(Deserialize)]
pub(super) struct UpdateOldestUserQueryParams {
  count: Option<usize>,
}

pub(crate) async fn update_oldest_user_inner() -> Result<(), APIError> {
  let pool = db_pool();
  let oldest_user: Option<i64> = sqlx::query_scalar(
    "SELECT osu_id FROM users WHERE osu_id > 2000000 ORDER BY last_updated ASC LIMIT 1;",
  )
  .fetch_optional(pool)
  .await
  .map_err(|err| {
    error!("Error fetching oldest user: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error fetching oldest user".to_owned(),
    }
  })?;

  let user_id = match oldest_user {
    Some(id) => id,
    None => {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: "No users found in database".to_owned(),
      });
    },
  };

  let query = "UPDATE users SET last_updated = CURRENT_TIMESTAMP WHERE osu_id = ?;";
  sqlx::query(query)
    .bind(user_id)
    .execute(pool)
    .await
    .map_err(|err| {
      error!("Error updating last_updated for user ID {user_id}: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Error updating last_updated for user".to_owned(),
      }
    })?;

  for mode in 0..=3 {
    let url = format!("https://ameobea.me/osutrack/api/get_changes.php?mode={mode}&id={user_id}");
    let res = match reqwest::get(&url).await {
      Ok(res) => res,
      Err(err) => {
        error!("Error fetching osu! API data for user ID {user_id}, mode {mode}: {err}");
        continue;
      },
    };
    if res.status() != StatusCode::OK {
      error!(
        "Non-200 response fetching osu! API data for user ID {user_id}, mode {mode}: status={}",
        res.status()
      );
      continue;
    }

    tokio::time::sleep(Duration::from_millis(1500)).await;
  }

  info!("Updated oldest user id={user_id}");

  Ok(())
}

pub(super) async fn update_oldest_user(
  Query(params): Query<UpdateOldestUserQueryParams>,
  admin_api_token: String,
) -> Result<(), APIError> {
  validate_admin_api_token(&admin_api_token)?;

  let count = params.count.unwrap_or(1).clamp(1, 100);
  for _ in 0..count {
    update_oldest_user_inner().await?;
    tokio::time::sleep(Duration::from_millis(1500)).await;
  }

  Ok(())
}
