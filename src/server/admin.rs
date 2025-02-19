use std::time::Duration;

use reqwest::StatusCode;

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
