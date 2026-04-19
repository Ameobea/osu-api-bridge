use std::{collections::HashSet, time::Duration};

use axum::{extract::Query, Json};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
  db::db_pool,
  osu_api::{fetch_user_hiscores, fetch_user_info, fetch_username, Ruleset},
};

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

pub(crate) async fn update_all_modes_for_user(user_id: u64) -> Result<(), APIError> {
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

  update_all_modes_for_user(user_id as u64).await?;

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

/// Restores a user's hiscores and updates from deleted tables back to active tables.
/// Does not re-add the user to the `users` table; that will happen on their next osutrack update.
async fn restore_deleted_user_data(user_id: i64) -> Result<(), APIError> {
  let pool = db_pool();
  let mut txn = pool.begin().await.map_err(|err| {
    error!("Error starting restore transaction for user id={user_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error starting transaction".to_owned(),
    }
  })?;

  sqlx::query(
    "INSERT IGNORE INTO hiscore_updates (user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic) SELECT user, beatmap_id, score, pp, mods, rank, score_time, \
     update_time, mode, is_classic FROM hiscore_updates_deleted WHERE user = ?",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error restoring hiscore_updates for user id={user_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error restoring hiscore updates".to_owned(),
    }
  })?;

  sqlx::query(
    "INSERT IGNORE INTO updates (user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode) SELECT user, count300, count100, count50, playcount, ranked_score, \
     total_score, pp_rank, level, pp_raw, accuracy, count_rank_ss, count_rank_s, count_rank_a, \
     timestamp, mode FROM updates_deleted WHERE user = ?",
  )
  .bind(user_id)
  .execute(&mut *txn)
  .await
  .map_err(|err| {
    error!("Error restoring updates for user id={user_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error restoring updates".to_owned(),
    }
  })?;

  txn.commit().await.map_err(|err| {
    error!("Error committing restore transaction for user id={user_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error committing restore transaction".to_owned(),
    }
  })?;

  Ok(())
}

#[derive(Serialize)]
pub struct DeletedUserReviewResult {
  pub user_id: i64,
  pub username: Option<String>,
  pub exists_on_osu: bool,
  /// True when the user already has some active rows in `hiscore_updates` (partially
  /// restored by a prior manual operation) but still has rows missing from the deleted tables.
  pub partially_restored: bool,
  /// Number of distinct hiscore rows in `hiscore_updates_deleted` not yet in
  /// `hiscore_updates` for this user+mode.
  pub missing_hiscore_count: usize,
  /// Number of rows in `updates_deleted` not yet in `updates` for this user+mode.
  pub missing_update_count: usize,
  /// The user's current pp for this mode as reported by the osu! API.
  pub current_pp: Option<f64>,
  /// MAX(pp_raw) across `updates_deleted` (and `updates` for partially-restored users).
  /// Used as the denominator for the pp ratio.
  pub historical_max_pp: Option<f64>,
  /// current_pp / historical_max_pp.  The primary restore signal when available.
  pub pp_ratio: Option<f64>,
  /// Populated only when the pp-ratio signal is unavailable and beatmap-overlap fallback
  /// is used instead (fully-deleted user with no updates history).
  pub current_hiscore_count: usize,
  pub matching_beatmap_count: usize,
  /// "restore" | "leave_deleted" | "uncertain" | "api_error"
  pub recommendation: String,
  /// True if a restore was actually performed (only possible when dry_run=false).
  pub restored: bool,
}

#[derive(Deserialize)]
pub(super) struct ReviewDeletedUsersQueryParams {
  /// Game mode to check (0=osu, 1=taiko, 2=ctb, 3=mania).  Defaults to 0.
  pub mode: Option<u8>,
  /// When true (default), only scan and report — do not write any changes to the DB.
  pub dry_run: Option<bool>,
  /// Minimum ratio of current_pp / historical_max_pp to recommend a restore.
  /// Users below this threshold likely had a score reset on unban.  Defaults to 0.90.
  pub min_pp_ratio: Option<f64>,
  /// Fallback minimum matching beatmap count used only when no pp history exists.
  /// Defaults to 1.
  pub min_matching_scores: Option<usize>,
}

/// Reviews users whose data was deleted by `verify_best_plays` and determines whether each
/// deletion was a false positive (API blip / transient ban) or a legitimate ban.
///
/// For every user with unrestored rows in the deleted tables, the endpoint:
///  1. Calls `GET /users/{id}/{mode}` to confirm existence and retrieve current pp.
///  2. Looks up `MAX(pp_raw)` from the deleted (and active, for partial restores) stats history.
///  3. **Primary signal — pp ratio**: `current_pp / historical_max_pp`.
///     - ≥ `min_pp_ratio` (default 0.75) → "restore" (pp is intact, API blip)
///     - < `min_pp_ratio` → "leave_deleted" (pp dropped sharply, likely score reset on unban)
///  4. **Fallback** (no pp history, fully-deleted user only): fetches current top 100 plays
///     and checks beatmap-ID overlap with deleted hiscores.
///  5. Partially-restored users with no pp history → "uncertain" (manual review needed).
///
/// Set `dry_run=false` to actually restore the recommended users.
pub(super) async fn review_deleted_users(
  Query(params): Query<ReviewDeletedUsersQueryParams>,
  admin_api_token: String,
) -> Result<Json<Vec<DeletedUserReviewResult>>, APIError> {
  validate_admin_api_token(&admin_api_token)?;

  let mode = params.mode.unwrap_or(0);
  let dry_run = params.dry_run.unwrap_or(true);
  let min_pp_ratio = params.min_pp_ratio.unwrap_or(0.90);
  let min_matching = params.min_matching_scores.unwrap_or(1);

  let ruleset = match Ruleset::from_mode_value(mode) {
    Some(r) => r,
    None => {
      return Err(APIError {
        status: StatusCode::BAD_REQUEST,
        message: format!("Invalid mode value: {mode}"),
      });
    },
  };

  let pool = db_pool();

  // Find all users that have any unrestored rows in either deleted table.
  //
  // Hiscore gap: a row in hiscore_updates_deleted with no matching (user, beatmap_id,
  // score_time) in hiscore_updates.
  //
  // Update gap: a row in updates_deleted with no matching (user, timestamp, mode) in
  // updates.
  //
  // Using UNION (not UNION ALL) dedups users that appear in both gaps.
  let users_with_gaps: Vec<i64> = sqlx::query_scalar(
    "SELECT user FROM ( \
       SELECT DISTINCT hud.user \
       FROM hiscore_updates_deleted hud \
       LEFT JOIN hiscore_updates hu \
         ON hu.user = hud.user AND hu.beatmap_id = hud.beatmap_id AND hu.score_time = hud.score_time \
       WHERE hud.mode = ? AND hu.user IS NULL \
       UNION \
       SELECT DISTINCT ud.user \
       FROM updates_deleted ud \
       LEFT JOIN updates u \
         ON u.user = ud.user AND u.timestamp = ud.timestamp AND u.mode = ud.mode \
       WHERE ud.mode = ? AND u.user IS NULL \
     ) combined",
  )
  .bind(mode)
  .bind(mode)
  .fetch_all(pool)
  .await
  .map_err(|err| {
    error!("Error fetching users with unrestored deleted data for mode={mode}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Error fetching users with unrestored deleted data".to_owned(),
    }
  })?;

  info!(
    "review_deleted_users: found {} users with unrestored deleted data for mode={mode} | \
     dry_run={dry_run} min_matching_scores={min_matching}",
    users_with_gaps.len()
  );

  let mut results: Vec<DeletedUserReviewResult> = Vec::new();

  for (i, &user_id) in users_with_gaps.iter().enumerate() {
    // Rate-limit between users (not before the very first one).
    if i > 0 {
      tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    info!(
      "[{}/{}] Checking user id={user_id}",
      i + 1,
      users_with_gaps.len()
    );

    // ── DB gap counts ────────────────────────────────────────────────────────
    let missing_hiscore_count: i64 = sqlx::query_scalar(
      "SELECT COUNT(*) FROM hiscore_updates_deleted hud \
       LEFT JOIN hiscore_updates hu \
         ON hu.user = hud.user AND hu.beatmap_id = hud.beatmap_id AND hu.score_time = hud.score_time \
       WHERE hud.user = ? AND hud.mode = ? AND hu.user IS NULL",
    )
    .bind(user_id)
    .bind(mode)
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let missing_update_count: i64 = sqlx::query_scalar(
      "SELECT COUNT(*) FROM updates_deleted ud \
       LEFT JOIN updates u \
         ON u.user = ud.user AND u.timestamp = ud.timestamp AND u.mode = ud.mode \
       WHERE ud.user = ? AND ud.mode = ? AND u.user IS NULL",
    )
    .bind(user_id)
    .bind(mode)
    .fetch_one(pool)
    .await
    .unwrap_or(0);

    let partially_restored: bool = sqlx::query_scalar(
      "SELECT EXISTS(SELECT 1 FROM hiscore_updates WHERE user = ? AND mode = ?)",
    )
    .bind(user_id)
    .bind(mode)
    .fetch_one(pool)
    .await
    .unwrap_or(false);

    // ── Historical max pp ────────────────────────────────────────────────────
    // Pull from both deleted and (for partial restores) active updates tables so that
    // recent post-restore improvements don't lower the effective baseline.
    let deleted_max_pp: Option<f64> = sqlx::query_scalar(
      "SELECT MAX(pp_raw) FROM updates_deleted WHERE user = ? AND mode = ?",
    )
    .bind(user_id)
    .bind(mode)
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    let active_max_pp: Option<f64> = if partially_restored {
      sqlx::query_scalar("SELECT MAX(pp_raw) FROM updates WHERE user = ? AND mode = ?")
        .bind(user_id)
        .bind(mode)
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
    } else {
      None
    };

    let historical_max_pp = match (deleted_max_pp, active_max_pp) {
      (Some(d), Some(a)) => Some(d.max(a)),
      (d, a) => d.or(a),
    };

    info!(
      "  user id={user_id}: missing_hiscores={missing_hiscore_count} \
       missing_updates={missing_update_count} partially_restored={partially_restored} \
       historical_max_pp={historical_max_pp:?}"
    );

    // ── Step 1: existence + current pp (single API call) ────────────────────
    let user_info = match fetch_user_info(user_id as u64, ruleset).await {
      Ok(Some(info)) => info,
      Ok(None) => {
        info!("  user id={user_id}: not found on osu! API → still banned; leaving deleted");
        results.push(DeletedUserReviewResult {
          user_id,
          username: None,
          exists_on_osu: false,
          partially_restored,
          missing_hiscore_count: missing_hiscore_count as usize,
          missing_update_count: missing_update_count as usize,
          current_pp: None,
          historical_max_pp,
          pp_ratio: None,
          current_hiscore_count: 0,
          matching_beatmap_count: 0,
          recommendation: "leave_deleted".to_owned(),
          restored: false,
        });
        continue;
      },
      Err(err) => {
        error!("  user id={user_id}: osu! API error: {err:?}");
        results.push(DeletedUserReviewResult {
          user_id,
          username: None,
          exists_on_osu: false,
          partially_restored,
          missing_hiscore_count: missing_hiscore_count as usize,
          missing_update_count: missing_update_count as usize,
          current_pp: None,
          historical_max_pp,
          pp_ratio: None,
          current_hiscore_count: 0,
          matching_beatmap_count: 0,
          recommendation: "api_error".to_owned(),
          restored: false,
        });
        continue;
      },
    };

    let current_pp = user_info.statistics.as_ref().and_then(|s| s.pp);
    let username = &user_info.username;

    info!(
      "  user id={user_id} username={username}: exists; \
       current_pp={current_pp:?} historical_max_pp={historical_max_pp:?}"
    );

    // ── Step 2: primary signal — pp ratio ───────────────────────────────────
    if let (Some(curr), Some(hist)) = (current_pp, historical_max_pp) {
      if hist > 0.0 {
        let ratio = curr / hist;
        let recommendation = if ratio >= min_pp_ratio {
          info!(
            "  user id={user_id} username={username}: pp_ratio={ratio:.3} \
             ({curr:.1}/{hist:.1}) ≥ {min_pp_ratio} → restore"
          );
          "restore"
        } else {
          info!(
            "  user id={user_id} username={username}: pp_ratio={ratio:.3} \
             ({curr:.1}/{hist:.1}) < {min_pp_ratio} → leave_deleted (likely score reset)"
          );
          "leave_deleted"
        };

        let restored = if !dry_run && recommendation == "restore" {
          info!("  user id={user_id} username={username}: performing restore (dry_run=false)");
          match restore_deleted_user_data(user_id).await {
            Ok(()) => {
              info!("  user id={user_id} username={username}: restore successful");
              true
            },
            Err(err) => {
              error!("  user id={user_id} username={username}: restore failed: {err:?}");
              false
            },
          }
        } else {
          false
        };

        results.push(DeletedUserReviewResult {
          user_id,
          username: Some(username.clone()),
          exists_on_osu: true,
          partially_restored,
          missing_hiscore_count: missing_hiscore_count as usize,
          missing_update_count: missing_update_count as usize,
          current_pp,
          historical_max_pp,
          pp_ratio: Some(ratio),
          current_hiscore_count: 0,
          matching_beatmap_count: 0,
          recommendation: recommendation.to_owned(),
          restored,
        });
        continue;
      }
    }

    // ── Step 3: fallback — beatmap overlap (fully-deleted, no pp history) ────
    // Partially-restored users without pp history can't be assessed automatically.
    if partially_restored {
      info!(
        "  user id={user_id} username={username}: no pp history for partially-restored \
         user → uncertain (manual review needed)"
      );
      results.push(DeletedUserReviewResult {
        user_id,
        username: Some(username.clone()),
        exists_on_osu: true,
        partially_restored: true,
        missing_hiscore_count: missing_hiscore_count as usize,
        missing_update_count: missing_update_count as usize,
        current_pp,
        historical_max_pp,
        pp_ratio: None,
        current_hiscore_count: 0,
        matching_beatmap_count: 0,
        recommendation: "uncertain".to_owned(),
        restored: false,
      });
      continue;
    }

    info!(
      "  user id={user_id} username={username}: no pp history; falling back to beatmap \
       overlap check"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    let missing_beatmap_ids: Vec<i64> = sqlx::query_scalar(
      "SELECT DISTINCT hud.beatmap_id \
       FROM hiscore_updates_deleted hud \
       LEFT JOIN hiscore_updates hu \
         ON hu.user = hud.user AND hu.beatmap_id = hud.beatmap_id AND hu.score_time = hud.score_time \
       WHERE hud.user = ? AND hud.mode = ? AND hu.user IS NULL",
    )
    .bind(user_id)
    .bind(mode)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let current_plays =
      match fetch_user_hiscores(user_id as u64, ruleset, Some(100), Some(0)).await {
        Ok(plays) => plays,
        Err(err) => {
          error!(
            "  user id={user_id} username={username}: error fetching hiscores for fallback \
             beatmap check: {err:?}"
          );
          results.push(DeletedUserReviewResult {
            user_id,
            username: Some(username.clone()),
            exists_on_osu: true,
            partially_restored: false,
            missing_hiscore_count: missing_hiscore_count as usize,
            missing_update_count: missing_update_count as usize,
            current_pp,
            historical_max_pp,
            pp_ratio: None,
            current_hiscore_count: 0,
            matching_beatmap_count: 0,
            recommendation: "api_error".to_owned(),
            restored: false,
          });
          continue;
        },
      };

    let current_beatmap_ids: HashSet<i64> =
      current_plays.iter().map(|p| p.beatmap_id).collect();
    let matching_count = missing_beatmap_ids
      .iter()
      .filter(|&&bm| current_beatmap_ids.contains(&bm))
      .count();

    info!(
      "  user id={user_id} username={username}: beatmap fallback — current_plays={} \
       missing_beatmaps={} matching={matching_count}",
      current_plays.len(),
      missing_beatmap_ids.len(),
    );

    let recommendation = if current_plays.is_empty() {
      info!("  user id={user_id} username={username}: 0 current plays → leave_deleted");
      "leave_deleted"
    } else if matching_count >= min_matching {
      info!(
        "  user id={user_id} username={username}: {matching_count} matching beatmaps ≥ \
         threshold {min_matching} → restore"
      );
      "restore"
    } else {
      info!(
        "  user id={user_id} username={username}: {matching_count} matching beatmaps < \
         threshold {min_matching} → uncertain"
      );
      "uncertain"
    };

    let restored = if !dry_run && recommendation == "restore" {
      info!("  user id={user_id} username={username}: performing restore (dry_run=false)");
      match restore_deleted_user_data(user_id).await {
        Ok(()) => {
          info!("  user id={user_id} username={username}: restore successful");
          true
        },
        Err(err) => {
          error!("  user id={user_id} username={username}: restore failed: {err:?}");
          false
        },
      }
    } else {
      false
    };

    results.push(DeletedUserReviewResult {
      user_id,
      username: Some(username.clone()),
      exists_on_osu: true,
      partially_restored: false,
      missing_hiscore_count: missing_hiscore_count as usize,
      missing_update_count: missing_update_count as usize,
      current_pp,
      historical_max_pp,
      pp_ratio: None,
      current_hiscore_count: current_plays.len(),
      matching_beatmap_count: matching_count,
      recommendation: recommendation.to_owned(),
      restored,
    });
  }

  info!(
    "review_deleted_users complete: total={} restore={} leave_deleted={} uncertain={} \
     api_error={}",
    results.len(),
    results.iter().filter(|r| r.recommendation == "restore").count(),
    results.iter().filter(|r| r.recommendation == "leave_deleted").count(),
    results.iter().filter(|r| r.recommendation == "uncertain").count(),
    results.iter().filter(|r| r.recommendation == "api_error").count(),
  );

  Ok(Json(results))
}
