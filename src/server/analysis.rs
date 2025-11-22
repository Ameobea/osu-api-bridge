use std::{
  sync::{atomic::AtomicBool, Arc},
  time::Duration,
};

use crate::{
  db::db_pool,
  osu_api::Ruleset,
  server::{admin::validate_admin_api_token, APIError},
};
use axum::{
  extract::Query,
  http::{header, StatusCode},
  response::{IntoResponse, Response},
  Json,
};
use chrono::NaiveDate;
use dashmap::DashMap;
use fxhash::{FxHashMap, FxHashSet};
use lazy_static::lazy_static;
use pco::{standalone::simple_compress, ChunkConfig, DeltaSpec, ModeSpec};
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use tokio::sync::{watch, RwLock};
use tracing::{error, info};

/// This holds data about values for decay rate and pp required to reach various ranks averaged over
/// the semi-recent past. This is designed to facilitate simulation of future rankings based on
/// current trends.
#[derive(Clone, Serialize)]
pub struct SimulationProfile {
  pub rank_to_decay: Vec<(u32, f32)>,
  pub rank_to_density: Vec<(u32, f32)>,
}

#[derive(Clone, Serialize)]
pub struct AnalysisData {
  pub dates: Vec<NaiveDate>,
  /// Rank buckets corresponding to columns in the pp/decay matrices.
  pub buckets: Vec<u32>,

  // Dense matrices flattened (row-major: date -> bucket)
  // Size = dates.len() * buckets.len()
  pub pp_matrix: Vec<f32>,
  pub decay_matrix: Vec<f32>,

  pub simulation_profile: SimulationProfile,

  /// Compressed binary representation of this analysis data for fast transfer over the API.
  pub binary_dataset: Vec<u8>,
}

lazy_static! {
  pub static ref ANALYSIS_CACHE: DashMap<Ruleset, Arc<AnalysisData>> = DashMap::new();
  /// If a request to get analysis data comes in before `ANALYSIS_CACHE` is populated, this is used
  /// to wait for it to be ready.
  ///
  /// If `tx` is `None`, an update is already in progress.  `ANALYSIS_CACHE` may or may not be initialized
  /// depending on whether this is the first update or not.
  pub static ref ANALYSIS_CACHE_READY: RwLock<(watch::Sender<bool>, watch::Receiver<bool>)> =
    RwLock::new(watch::channel(false));
}

static IS_ANALYSIS_CACHE_UPDATING: AtomicBool = AtomicBool::new(false);

async fn update_analysis_data_inner() -> Result<(), APIError> {
  let mut res = Ok(());

  for mode in Ruleset::ALL {
    let mut attempts = 0;
    loop {
      match rebuild_analysis_data(mode).await {
        Ok(data) => {
          ANALYSIS_CACHE.insert(mode, Arc::new(data));
          info!("Updated analysis data for {mode:?}");
          break;
        },
        Err(err) => {
          attempts += 1;
          error!("Failed to update analysis data for {mode:?} (attempt {attempts}): {err:?}");
          if attempts >= 3 {
            error!("Giving up on updating analysis data for {mode:?} after {attempts} attempts");
            res = Err(err);
            break;
          }

          tokio::time::sleep(Duration::from_secs(5)).await;
        },
      }
    }
  }

  res
}

pub async fn update_analysis_data() -> Result<(), APIError> {
  if IS_ANALYSIS_CACHE_UPDATING
    .compare_exchange(
      false,
      true,
      std::sync::atomic::Ordering::SeqCst,
      std::sync::atomic::Ordering::SeqCst,
    )
    .is_err()
  {
    return Err(APIError {
      status: StatusCode::TOO_MANY_REQUESTS,
      message: "Analysis data update already in progress".to_owned(),
    });
  }

  let mut lock = ANALYSIS_CACHE_READY.write().await;
  let tx = &mut lock.0;

  let res = update_analysis_data_inner().await;

  let _ = tx.send(true); // Notify waiters that the cache is ready
  IS_ANALYSIS_CACHE_UPDATING.store(false, std::sync::atomic::Ordering::SeqCst);

  res
}

async fn get_analysis_cache(mode: Ruleset) -> Result<Arc<AnalysisData>, APIError> {
  if let Some(entry) = ANALYSIS_CACHE.get(&mode) {
    return Ok(entry.value().clone());
  }

  // Wait for the cache to be ready
  let mut rx = {
    let lock = ANALYSIS_CACHE_READY.read().await;
    lock.1.clone()
  };
  if *rx.borrow_and_update() {
    if let Some(entry) = ANALYSIS_CACHE.get(&mode) {
      return Ok(entry.value().clone());
    } else {
      error!("Analysis data not available after cache signaled readiness (1)");
      return Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Analysis data not available after cache signaled readiness (1)".to_owned(),
      });
    }
  }

  rx.changed().await.map_err(|_| {
    error!("analysis cache tx went away; update task likely panicked");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Analysis cache update failed".to_owned(),
    }
  })?;
  if let Some(entry) = ANALYSIS_CACHE.get(&mode) {
    Ok(entry.value().clone())
  } else {
    error!("Analysis data not available even after waiting for cache readiness (2)");
    Err(APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Analysis data not available even after waiting for cache readiness (2)".to_owned(),
    })
  }
}

/// Half-width of the rank bucket window used to select samples around a target rank, relative to
/// the target rank.
const WINDOW_RATIO: f32 = 0.01;
/// Minimum half-width of the rank bucket window.
const MIN_WINDOW_WIDTH: u32 = 1;
/// Maximum half-width of the rank bucket window.
const MAX_WINDOW_WIDTH: u32 = 20_000;

fn bucket_half_width(rank: u32) -> u32 {
  // the top 100 buckets are exact
  if rank < 100 {
    return 0;
  }

  let base = (rank as f32 * WINDOW_RATIO).round() as u32;
  base.clamp(MIN_WINDOW_WIDTH, MAX_WINDOW_WIDTH)
}

/// Upper bound on ranks we care about when emitting history curves.
pub const MAX_TRACKED_RANK: u32 = 2_500_000;
/// Geometric growth factor between sparse rank buckets.
pub const TARGET_RANK_GROWTH: f64 = 1.0275;
/// Use linear buckets for the very top of the leaderboard where decay changes rapidly.
pub const DENSE_RANK_UPPER_BOUND: u32 = 100;

lazy_static! {
  /// Defines the center of the buckets that are sampled from the full leaderboard to estimate the pp/rank/time surface.
  pub static ref TARGET_RANKS: Vec<u32> = build_target_ranks();
}

fn build_target_ranks() -> Vec<u32> {
  let mut ranks = Vec::new();
  for rank in 1..=DENSE_RANK_UPPER_BOUND {
    ranks.push(rank);
  }

  let mut current = DENSE_RANK_UPPER_BOUND as f64;
  while current < MAX_TRACKED_RANK as f64 {
    current *= TARGET_RANK_GROWTH;
    let mut next = current.round() as u32;
    if next <= *ranks.last().unwrap_or(&0) {
      next = ranks.last().copied().unwrap_or(0) + 1;
    }
    if next > MAX_TRACKED_RANK {
      break;
    }
    ranks.push(next);
  }

  if ranks.last().copied() != Some(MAX_TRACKED_RANK) {
    ranks.push(MAX_TRACKED_RANK);
  }

  ranks
}

pub fn target_ranks() -> &'static [u32] { &TARGET_RANKS }

#[derive(Debug, FromRow)]
pub struct RankSample {
  pub rank: u32,
  pub pp: f32,
}

/// Represents a sparse snapshot with optional PP values for each rank bucket.
#[derive(Debug, Clone)]
pub struct SparseSnapshot {
  pub date: NaiveDate,
  pub mode: Ruleset,
  /// PP values for each rank bucket. None means no data was available for that bucket on this
  /// date.
  pub bucket_values: Vec<Option<f32>>,
  pub rows_scanned: usize,
  pub buckets_from_samples: usize,
}

#[derive(Debug, FromRow)]
pub struct RankSampleWithDate {
  pub date: NaiveDate,
  pub rank: u32,
  pub pp: f32,
}

/// Given a set of rank samples that fall within a rank bucket, estimates the PP at the center of
/// the bucket (`target_rank`).
///
/// This is done by assuming that the PP vs Rank relationship within the bucket is roughly linear
/// and fitting a line to the samples then using that line to compute the PP at the target rank.
fn estimate_bucket_center_pp(samples: &[RankSample], target_rank: u32) -> f32 {
  if samples.is_empty() {
    return 0.;
  }

  if samples.len() == 1 {
    return samples[0].pp;
  }

  // A line requires at least two unique rank points to fit.  If we don't have that, the best we can
  // do is just average the PP values.
  //
  // It might be possible to do something more sophisticated by looking at neighboring buckets along
  // either/both the X/Y axis, but that's not really necessary for now thanks to the filtering
  // that's done on the resulting matrix before further analysis.
  let first_rank = samples[0].rank;
  let more_than_one_unique_rank = samples.iter().skip(1).all(|s| s.rank != first_rank);
  if !more_than_one_unique_rank {
    return samples.iter().map(|s| s.pp).sum::<f32>() / samples.len() as f32;
  }

  let n = samples.len() as f32;
  let sum_rank: f32 = samples.iter().map(|s| s.rank as f32).sum();
  let sum_pp: f32 = samples.iter().map(|s| s.pp).sum();
  let sum_rank_pp: f32 = samples.iter().map(|s| s.rank as f32 * s.pp).sum();
  let sum_rank_sq: f32 = samples.iter().map(|s| (s.rank as f32).powi(2)).sum();

  let mean_rank = sum_rank / n;
  let mean_pp = sum_pp / n;

  let numerator = n * sum_rank_pp - sum_rank * sum_pp;
  let denominator = n * sum_rank_sq - sum_rank.powi(2);

  if denominator.abs() < 1e-10 {
    return mean_pp;
  }

  // solving for y = mx + b
  let slope = numerator / denominator;
  let intercept = mean_pp - slope * mean_rank;

  slope * target_rank as f32 + intercept
}

/// Compute a sparse snapshot from raw samples without any interpolation.
/// Missing rank buckets are left as None.
fn compute_sparse_snapshot(
  samples: &[RankSample],
  mode: Ruleset,
  date: NaiveDate,
) -> Option<SparseSnapshot> {
  if samples.is_empty() {
    return None;
  }

  let targets = target_ranks();
  let mut bucket_values: Vec<Option<f32>> = vec![None; targets.len()];
  let mut actual_buckets = 0usize;

  let mut start_ix = 0usize;
  let mut end_ix = 0usize;

  for (bucket_ix, &target_rank) in targets.iter().enumerate() {
    let window = bucket_half_width(target_rank);
    let (lower, upper) = if window == 0 {
      (target_rank, target_rank)
    } else {
      (
        target_rank.saturating_sub(window),
        target_rank.saturating_add(window),
      )
    };

    while start_ix < samples.len() && samples[start_ix].rank < lower {
      start_ix += 1;
    }
    if end_ix < start_ix {
      end_ix = start_ix;
    }
    while end_ix < samples.len() && samples[end_ix].rank <= upper {
      end_ix += 1;
    }

    let slice = &samples[start_ix..end_ix];
    if slice.is_empty() {
      continue;
    }

    let pp_at_bucket_center = estimate_bucket_center_pp(slice, target_rank);
    bucket_values[bucket_ix] = Some(pp_at_bucket_center);
    actual_buckets += 1;
  }

  if actual_buckets == 0 {
    return None;
  }

  Some(SparseSnapshot {
    date,
    mode,
    bucket_values,
    rows_scanned: samples.len(),
    buckets_from_samples: actual_buckets,
  })
}

pub async fn fetch_rank_samples(mode: Ruleset) -> Result<Vec<RankSampleWithDate>, APIError> {
  let pool = db_pool();

  info!("Fetching (rank, pp) samples for mode={mode:?}");

  sqlx::query_as(
    "SELECT DATE(timestamp) as date, CAST(pp_rank as UNSIGNED) as rank, pp_raw as pp FROM updates \
     WHERE mode = ? AND pp_rank > 0 AND pp_raw > 0 ORDER BY date ASC, pp_rank ASC",
  )
  .bind(mode.mode_value())
  .fetch_all(pool)
  .await
  .map_err(|err| {
    error!("Failed to fetch updates for rank-decay snapshots for mode={mode:?}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch updates for snapshots".to_owned(),
    }
  })
}

/// Interpolate missing values across the time axis (X-axis) for a single bucket index.
/// For ranks <= 1000: Linear interpolation between previous and next samples, or carry last value
/// forward. For ranks > 1000: Also attempt Y-axis interpolation from adjacent buckets if available.
fn interpolate_bucket_across_time(
  snapshots: &[SparseSnapshot],
  bucket_idx: usize,
  rank: u32,
) -> Vec<Option<f32>> {
  let num_snapshots = snapshots.len();
  let mut result = vec![None; num_snapshots];

  // First, extract all existing values for this bucket across all snapshots.
  for (snapshot_idx, snapshot) in snapshots.iter().enumerate() {
    result[snapshot_idx] = snapshot.bucket_values[bucket_idx];
  }

  // Now perform time-domain (X-axis) interpolation.
  let mut snapshot_idx = 0;
  let mut last_known_idx: Option<usize> = result.iter().position(|v| v.is_some());

  if last_known_idx.is_none() {
    // No samples at all for this bucket. Try Y-axis interpolation for ranks > 1000.
    if rank > 1000 {
      for (snapshot_idx, snapshot) in snapshots.iter().enumerate() {
        // Try adjacent buckets (one above and one below).
        let prev_bucket_val = if bucket_idx > 0 {
          snapshot.bucket_values[bucket_idx - 1]
        } else {
          None
        };
        let next_bucket_val = if bucket_idx + 1 < snapshot.bucket_values.len() {
          snapshot.bucket_values[bucket_idx + 1]
        } else {
          None
        };

        // Use average of adjacent buckets if both exist.
        result[snapshot_idx] = match (prev_bucket_val, next_bucket_val) {
          (Some(prev), Some(next)) => Some((prev + next) / 2.),
          (Some(val), None) | (None, Some(val)) => Some(val),
          (None, None) => None,
        };
      }
    }
    return result;
  }

  // Fill leading gap with first known value.
  if let Some(first_idx) = last_known_idx {
    for idx in 0..first_idx {
      result[idx] = result[first_idx];
    }
    snapshot_idx = first_idx + 1;
  }

  // Interpolate middle gaps.
  while snapshot_idx < num_snapshots {
    if result[snapshot_idx].is_some() {
      last_known_idx = Some(snapshot_idx);
      snapshot_idx += 1;
      continue;
    }

    // Find the next known value.
    let next_known_idx = ((snapshot_idx + 1)..num_snapshots).find(|&idx| result[idx].is_some());

    match (last_known_idx, next_known_idx) {
      (Some(prev_idx), Some(next_idx)) => {
        let prev_val = result[prev_idx].unwrap();
        let next_val = result[next_idx].unwrap();
        let time_span = (next_idx - prev_idx) as f32;

        // Linear interpolation across time.
        for gap_idx in prev_idx + 1..next_idx {
          let t = ((gap_idx - prev_idx) as f32 / time_span).clamp(0., 1.);
          result[gap_idx] = Some(prev_val + t * (next_val - prev_val));
        }

        last_known_idx = Some(next_idx);
        snapshot_idx = next_idx + 1;
      },
      (Some(prev_idx), None) => {
        // Tail gap: carry last known value forward.
        let prev_val = result[prev_idx].unwrap();
        for idx in prev_idx + 1..num_snapshots {
          result[idx] = Some(prev_val);
        }
        break;
      },
      _ => break,
    }
  }

  result
}

pub fn interpolate_snapshots_across_time(snapshots: &[SparseSnapshot]) -> Vec<SparseSnapshot> {
  if snapshots.is_empty() {
    return Vec::new();
  }

  let num_buckets = snapshots[0].bucket_values.len();
  let ranks = target_ranks();

  info!(
    "Starting time-domain interpolation for {} snapshots across {num_buckets} rank buckets",
    snapshots.len(),
  );

  // Create result snapshots with the same structure.
  let mut interpolated_snapshots: Vec<SparseSnapshot> = snapshots
    .iter()
    .map(|s| SparseSnapshot {
      date: s.date,
      mode: s.mode,
      bucket_values: vec![None; num_buckets],
      rows_scanned: s.rows_scanned,
      buckets_from_samples: s.buckets_from_samples,
    })
    .collect();

  // Process each bucket independently across time.
  for bucket_idx in 0..num_buckets {
    let rank = ranks[bucket_idx];
    let interpolated_values = interpolate_bucket_across_time(snapshots, bucket_idx, rank);

    // Copy interpolated values back to the result snapshots.
    for (snapshot_idx, value) in interpolated_values.into_iter().enumerate() {
      interpolated_snapshots[snapshot_idx].bucket_values[bucket_idx] = value;
    }
  }

  info!("Completed time-domain interpolation");

  interpolated_snapshots
}

fn detect_shocks(
  pp_matrix: &[f32],
  dates: &[NaiveDate],
  buckets: &[u32],
  threshold_z: f32,
) -> Vec<usize> {
  let ranks = [1000, 2000, 50000, 200000];
  let max_rank = buckets.last().copied().unwrap_or(0);

  let mut global_shocks = FxHashSet::default();

  for r in ranks {
    if r > max_rank {
      continue;
    }

    let pp_hist = get_history_for_rank(r, pp_matrix, dates, buckets);
    // Calculate percentage change day-over-day
    let mut pct_changes = Vec::with_capacity(pp_hist.len());
    for i in 0..pp_hist.len() {
      if i == 0 {
        pct_changes.push(0.);
        continue;
      }

      let prev = pp_hist[i - 1].1;
      let curr = pp_hist[i].1;
      if prev.abs() < 1e-6 {
        pct_changes.push(0.);
      } else {
        pct_changes.push((curr - prev) / prev);
      }
    }

    let n = pct_changes.len() as f32;
    let mean: f32 = pct_changes.iter().sum::<f32>() / n;
    // Use sample standard deviation (N-1) to match Pandas default
    let std_dev: f32 = if n > 1.0 {
      (pct_changes.iter().map(|&x| (x - mean).powi(2)).sum::<f32>() / (n - 1.0)).sqrt()
    } else {
      0.0
    };

    for (idx, &pct_change) in pct_changes.iter().enumerate() {
      let z_score = if std_dev.abs() < 1e-6 {
        0.
      } else {
        (pct_change - mean) / std_dev
      };

      if z_score.abs() > threshold_z {
        global_shocks.insert(idx);
      }
    }
  }

  let mut shock_list: Vec<usize> = global_shocks.into_iter().collect();
  shock_list.sort_unstable();
  shock_list
}

pub async fn rebuild_analysis_data(mode: Ruleset) -> Result<AnalysisData, APIError> {
  let samples = fetch_rank_samples(mode).await.map_err(|e| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Failed to fetch samples: {}", e.message),
  })?;

  let mut samples_by_date: FxHashMap<NaiveDate, Vec<RankSample>> = FxHashMap::default();
  for sample in samples {
    samples_by_date
      .entry(sample.date)
      .or_default()
      .push(RankSample {
        rank: sample.rank,
        pp: sample.pp,
      });
  }

  if samples_by_date.is_empty() {
    return Err(APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "No rank samples available for analysis".to_owned(),
    });
  }

  // let mut all_dates = samples_by_date.keys().cloned().collect::<Vec<NaiveDate>>();
  // all_dates.sort_unstable();

  // Just in case there was somehow a date gap in the samples, fill in missing dates with empty
  // samples. let first_date = *samples_by_date.keys().min().unwrap();
  // let last_date = *samples_by_date.keys().max().unwrap();

  let (first_date, last_date) = samples_by_date.keys().fold(
    (
      NaiveDate::from_ymd_opt(3000, 1, 1).unwrap(),
      NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
    ),
    |(min_date, max_date), &date| {
      (
        if date < min_date { date } else { min_date },
        if date > max_date { date } else { max_date },
      )
    },
  );

  let mut all_dates = Vec::new();
  let mut current_date = first_date;
  while current_date <= last_date {
    all_dates.push(current_date);
    current_date = current_date.succ_opt().unwrap();
  }

  let mut sparse_snapshots = Vec::new();
  for &date in &all_dates {
    let Some(daily_samples) = samples_by_date.get(&date) else {
      sparse_snapshots.push(SparseSnapshot {
        date,
        mode,
        bucket_values: vec![None; target_ranks().len()],
        rows_scanned: 0,
        buckets_from_samples: 0,
      });
      continue;
    };

    if let Some(sparse) = compute_sparse_snapshot(daily_samples, mode, date) {
      sparse_snapshots.push(sparse);
    } else {
      sparse_snapshots.push(SparseSnapshot {
        date,
        mode,
        bucket_values: vec![None; target_ranks().len()],
        rows_scanned: 0,
        buckets_from_samples: 0,
      });
    }
  }

  let interpolated = interpolate_snapshots_across_time(&sparse_snapshots);

  // Convert to dense matrix
  let buckets = target_ranks().to_vec();
  let num_buckets = buckets.len();
  let num_days = all_dates.len();
  let mut pp_matrix = Vec::with_capacity(num_days * buckets.len());

  for snapshot in &interpolated {
    for val in &snapshot.bucket_values {
      pp_matrix.push(val.unwrap_or(0.));
    }
  }

  let shocks = detect_shocks(&pp_matrix, &all_dates, &buckets, 3.);

  let smoothed_pp_matrix = smooth_matrix(&pp_matrix, num_buckets, num_days, &shocks, 7);

  let decay_matrix = calculate_decay(&smoothed_pp_matrix, &buckets, num_days, &shocks, 24);

  let simulation_profile =
    generate_simulation_profile(&smoothed_pp_matrix, &buckets, num_days, &shocks);

  let mut data = AnalysisData {
    dates: all_dates,
    buckets,
    pp_matrix: smoothed_pp_matrix,
    decay_matrix,
    simulation_profile,
    binary_dataset: Vec::new(),
  };
  data.binary_dataset = data.build_binary_dataset()?;

  Ok(data)
}

fn smooth_matrix(
  pp_matrix: &[f32],
  num_buckets: usize,
  num_days: usize,
  shocks: &[usize],
  window_radius: usize,
) -> Vec<f32> {
  let mut smoothed = vec![0.; pp_matrix.len()];

  for i in 0..num_days {
    let mut start_idx = i.saturating_sub(window_radius);
    let mut end_idx = (i + window_radius).min(num_days - 1);

    // Check for shocks in the future part of the window
    for &shock in shocks {
      if i < shock && shock <= end_idx {
        end_idx = shock - 1;
        break;
      }
    }

    // Check for shocks in the past part of the window
    for &shock in shocks.iter().rev() {
      if start_idx <= shock && shock < i {
        start_idx = shock;
        break;
      }
    }

    let window_len = end_idx - start_idx + 1;
    let window_size = window_len as f32;
    let mut weights = Vec::with_capacity(window_len);
    let mut weight_sum = 0.;

    for j in 0..window_len {
      let day = start_idx + j;
      let dist = (day as isize - i as isize).abs() as f32;
      let weight = 1. - dist / window_size;
      weights.push(weight);
      weight_sum += weight;
    }

    for bucket in 0..num_buckets {
      let mut weighted_sum = 0.;
      for (j, &weight) in weights.iter().enumerate() {
        let day = start_idx + j;
        let val = pp_matrix[day * num_buckets + bucket];
        weighted_sum += val * weight;
      }
      smoothed[i * num_buckets + bucket] = weighted_sum / weight_sum;
    }
  }

  smoothed
}

fn linear_slope(points: &[(f32, f32)]) -> f32 {
  let n = points.len() as f32;
  let sum_x: f32 = points.iter().map(|p| p.0).sum();
  let sum_y: f32 = points.iter().map(|p| p.1).sum();
  let sum_xy: f32 = points.iter().map(|p| p.0 * p.1).sum();
  let sum_xx: f32 = points.iter().map(|p| p.0 * p.0).sum();

  let numerator = n * sum_xy - sum_x * sum_y;
  let denominator = n * sum_xx - sum_x * sum_x;

  if denominator.abs() < 1e-9 {
    0.
  } else {
    numerator / denominator
  }
}

fn calculate_decay(
  pp_matrix: &[f32],
  buckets: &[u32],
  num_days: usize,
  shocks: &[usize],
  window_radius: usize,
) -> Vec<f32> {
  let num_buckets = buckets.len();
  // Initialize with NaN so that missing/skipped days are not treated as 0 decay
  let mut decay_matrix = vec![f32::NAN; pp_matrix.len()];

  for day in 0..num_days {
    // Determine window [start_idx, end_idx]
    let mut start_idx = day.saturating_sub(window_radius);
    let mut end_idx = (day + window_radius).min(num_days - 1);

    // Windows are clamped by shocks.  This helps reduce the impact of pp reworks, inactive account
    // purges, or other standalone events from getting smeared into the natural daily decay rate
    // caused by players farming.

    // Check for shocks in future
    for &shock in shocks {
      if day < shock && shock <= end_idx {
        end_idx = shock - 1;
        break;
      }
    }
    // Check for shocks in past
    for &shock in shocks.iter().rev() {
      if start_idx <= shock && shock < day {
        start_idx = shock;
        break;
      }
    }

    if end_idx < start_idx + 3 {
      continue;
    }

    for bucket_idx in 0..num_buckets {
      let current_pp = pp_matrix[day * num_buckets + bucket_idx];

      // Collect (x, y) points
      let mut points = Vec::with_capacity(end_idx - start_idx + 1);
      for d in start_idx..=end_idx {
        let pp_row = &pp_matrix[d * num_buckets..(d + 1) * num_buckets];
        let rank = get_rank_for_pp(pp_row, buckets, current_pp);
        points.push((d as f32, rank));
      }

      // Linear regression
      if points.len() > 1 {
        let mut slope = linear_slope(&points);

        if bucket_idx == 0 {
          if slope < 0. {
            slope = 0.;
          }
        }

        decay_matrix[day * num_buckets + bucket_idx] = slope;
      }
    }
  }

  decay_matrix
}

fn get_rank_for_pp(pp_row: &[f32], buckets: &[u32], target_pp: f32) -> f32 {
  let num_buckets = buckets.len();
  // pp_row is descending (Rank 1 has high PP)
  // Find first index where PP < target_pp
  let idx = pp_row.partition_point(|&pp| pp > target_pp);

  if idx == 0 {
    1.0
  } else if idx >= num_buckets {
    buckets[num_buckets - 1] as f32
  } else {
    let pp_high = pp_row[idx - 1]; // Higher PP, lower rank value
    let pp_low = pp_row[idx]; // Lower PP, higher rank value
    let rank_high = buckets[idx - 1] as f32; // Lower rank value
    let rank_low = buckets[idx] as f32; // Higher rank value

    if (pp_high - pp_low).abs() < 1e-6 {
      rank_high
    } else {
      // Interpolation weight (linear in PP domain)
      let w = (target_pp - pp_high) / (pp_low - pp_high);

      // Map to log-rank domain
      let log_rank_high = rank_high.ln();
      let log_rank_low = rank_low.ln();
      let log_result = log_rank_high + w * (log_rank_low - log_rank_high);

      log_result.exp()
    }
  }
}

fn generate_simulation_profile(
  pp_matrix: &[f32],
  buckets: &[u32],
  num_days: usize,
  shocks: &[usize],
) -> SimulationProfile {
  let num_buckets = buckets.len();
  let lookback = 180.min(num_days);
  let start_day = num_days.saturating_sub(lookback);

  let mut rank_to_decay = Vec::with_capacity(num_buckets);
  let mut rank_to_pp = Vec::with_capacity(num_buckets);
  let shock_day_set: FxHashSet<usize> = shocks.iter().copied().collect();

  for bucket_idx in 0..num_buckets {
    let rank = buckets[bucket_idx];

    // Decay is a noisy value so we want to average that, but we want to use the rank to pp profile
    // from the most recent day since that's much more stable.
    let latest_pp = pp_matrix[(num_days - 1) * num_buckets + bucket_idx];
    rank_to_pp.push((rank, latest_pp));

    // Calculate decay by finding the slope of the rank trajectory for a fixed PP value.
    let start_pp = pp_matrix[start_day * num_buckets + bucket_idx];
    let mut points = Vec::new();
    for day in start_day..num_days {
      if shock_day_set.contains(&day) {
        continue;
      }

      let pp_row = &pp_matrix[day * num_buckets..(day + 1) * num_buckets];
      let r_actual = get_rank_for_pp(pp_row, buckets, start_pp);
      let day_index_for_regression = points.len() as f32;
      points.push((day_index_for_regression, r_actual));
    }

    let slope = if points.len() > 1 {
      linear_slope(&points)
    } else {
      0.
    };
    rank_to_decay.push((rank, slope));
  }

  let mut rank_to_density = Vec::with_capacity(num_buckets);
  for i in 0..num_buckets {
    let rank = buckets[i];
    let ranks_per_pp = if i < num_buckets - 1 {
      let (_r_curr, pp_curr) = rank_to_pp[i];
      let (r_next, pp_next) = rank_to_pp[i + 1];

      let rank_diff = (r_next - rank) as f32;
      let pp_diff = pp_curr - pp_next;

      if pp_diff.abs() > 1e-6 {
        let density = rank_diff / pp_diff;
        if density.is_finite() && density >= 0. {
          density
        } else if let Some(&(_, last_density)) = rank_to_density.last() {
          last_density
        } else {
          0.
        }
      } else if let Some(&(_, last_density)) = rank_to_density.last() {
        last_density
      } else {
        0.
      }
    } else if let Some(&(_, last_density)) = rank_to_density.last() {
      last_density
    } else {
      0.
    };
    rank_to_density.push((rank, ranks_per_pp));
  }

  SimulationProfile {
    rank_to_decay,
    rank_to_density,
  }
}

#[derive(Deserialize)]
pub struct AnalysisQuery {
  mode: Ruleset,
}

pub async fn get_analysis_dataset(
  Query(params): Query<AnalysisQuery>,
) -> Result<Response, APIError> {
  let data = get_analysis_cache(params.mode).await?;

  let buffer = data.binary_dataset.clone();

  Ok(([(header::CONTENT_TYPE, "application/octet-stream")], buffer).into_response())
}

pub async fn get_simulation_config(
  Query(params): Query<AnalysisQuery>,
) -> Result<Json<SimulationProfile>, APIError> {
  let data = get_analysis_cache(params.mode).await?;

  Ok(Json(data.simulation_profile.clone()))
}

#[derive(Deserialize)]
pub struct SanityCheckQuery {
  mode: Ruleset,
  rank: u32,
}

pub async fn refresh_analysis_data(
  admin_api_token: String,
) -> Result<Json<&'static str>, APIError> {
  validate_admin_api_token(&admin_api_token)?;

  update_analysis_data().await?;
  Ok(Json("Success"))
}

/// Given a matrix (pp or decay), extract the history for a specific rank by interpolating between
/// the buckets on either side of it.
fn get_history_for_rank(
  rank: u32,
  matrix: &[f32],
  dates: &[NaiveDate],
  buckets: &[u32],
) -> Vec<(NaiveDate, f32)> {
  let mut result = Vec::with_capacity(dates.len());
  let num_buckets = buckets.len();

  // Find buckets surrounding the target rank
  let bucket_idx = buckets.partition_point(|&r| r < rank);

  // Handle edge cases
  let (idx_low, idx_high, w) = if bucket_idx == 0 {
    (0, 0, 0.)
  } else if bucket_idx >= num_buckets {
    (num_buckets - 1, num_buckets - 1, 0.)
  } else if buckets[bucket_idx] == rank {
    (bucket_idx, bucket_idx, 0.)
  } else {
    let idx_lower = bucket_idx - 1;
    let idx_upper = bucket_idx;

    let r_lower = buckets[idx_lower] as f32;
    let r_upper = buckets[idx_upper] as f32;
    let target = rank as f32;

    // Log-Linear Interpolation
    let log_target = target.ln();
    let log_lower = r_lower.ln();
    let log_upper = r_upper.ln();

    let w = if (log_upper - log_lower).abs() < 1e-9 {
      0.
    } else {
      (log_target - log_lower) / (log_upper - log_lower)
    };
    (idx_lower, idx_upper, w)
  };

  for (day_idx, date) in dates.iter().enumerate() {
    let value_low = matrix[day_idx * num_buckets + idx_low];
    let value_high = matrix[day_idx * num_buckets + idx_high];
    let value = (1. - w) * value_low + w * value_high;
    result.push((*date, value));
  }

  result
}

impl AnalysisData {
  fn get_pp_history_for_rank(&self, rank: u32) -> Vec<(NaiveDate, f32)> {
    get_history_for_rank(rank, &self.pp_matrix, &self.dates, &self.buckets)
  }

  fn get_decay_history_for_rank(&self, rank: u32) -> Vec<(NaiveDate, f32)> {
    get_history_for_rank(rank, &self.decay_matrix, &self.dates, &self.buckets)
  }

  fn build_binary_dataset(&self) -> Result<Vec<u8>, APIError> {
    let mut buffer = Vec::new();

    let start_date_epoch: i64 = self
      .dates
      .first()
      .map(|d| d.and_hms_opt(4, 20, 0).unwrap().and_utc().timestamp())
      .unwrap_or(0);
    let days_count = self.dates.len() as u32;

    buffer.extend_from_slice(&start_date_epoch.to_le_bytes());
    buffer.extend_from_slice(&days_count.to_le_bytes());

    let base_pco_config = ChunkConfig::default()
      .with_compression_level(10)
      .with_mode_spec(ModeSpec::Auto)
      .with_delta_spec(DeltaSpec::Auto);

    let compressed_rank_buckets =
      simple_compress(&self.buckets, &base_pco_config).map_err(|err| {
        error!("Failed to compress rank buckets: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to compress rank buckets".to_owned(),
        }
      })?;
    let rank_buckets_before_size = (self.buckets.len() * std::mem::size_of::<u32>()) as u32;
    let rank_buckets_after_size = compressed_rank_buckets.len() as u32;
    info!(
      "Rank buckets compressed from {} bytes to {} bytes (ratio {:.2}%)",
      rank_buckets_before_size,
      rank_buckets_after_size,
      (rank_buckets_after_size as f64 / rank_buckets_before_size as f64) * 100.
    );

    let mut truncated_pp_matrix = Vec::with_capacity(self.pp_matrix.len());
    for &val in &self.pp_matrix {
      let val = (val * 100.) as u32;
      truncated_pp_matrix.push(val);
    }

    let compressed_pp_matrix =
      simple_compress(&truncated_pp_matrix, &base_pco_config).map_err(|err| {
        error!("Failed to compress pp matrix: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to compress pp matrix".to_owned(),
        }
      })?;
    let pp_matrix_before_size = (self.pp_matrix.len() * std::mem::size_of::<f32>()) as u32;
    let pp_matrix_after_size = compressed_pp_matrix.len() as u32;
    info!(
      "PP matrix compressed from {} bytes to {} bytes (ratio {:.2}%)",
      pp_matrix_before_size,
      pp_matrix_after_size,
      (pp_matrix_after_size as f64 / pp_matrix_before_size as f64) * 100.
    );

    let mut truncated_decay_matrix = Vec::with_capacity(self.decay_matrix.len());
    for &val in &self.decay_matrix {
      let val = (val * 100.) as i32;
      truncated_decay_matrix.push(val);
    }

    let compressed_decay_matrix = simple_compress(&truncated_decay_matrix, &base_pco_config)
      .map_err(|err| {
        error!("Failed to compress decay matrix: {err}");
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to compress decay matrix".to_owned(),
        }
      })?;
    let decay_matrix_before_size = (self.decay_matrix.len() * std::mem::size_of::<f32>()) as u32;
    let decay_matrix_after_size = compressed_decay_matrix.len() as u32;
    info!(
      "Decay matrix compressed from {} bytes to {} bytes (ratio {:.2}%)",
      decay_matrix_before_size,
      decay_matrix_after_size,
      (decay_matrix_after_size as f64 / decay_matrix_before_size as f64) * 100.
    );

    buffer.extend_from_slice(&(compressed_rank_buckets.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&compressed_rank_buckets);
    buffer.extend_from_slice(&(compressed_pp_matrix.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&compressed_pp_matrix);
    buffer.extend_from_slice(&(compressed_decay_matrix.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&compressed_decay_matrix);

    Ok(buffer)
  }
}

pub async fn get_sanity_pp(
  Query(params): Query<SanityCheckQuery>,
) -> Result<Json<Vec<(NaiveDate, f32)>>, APIError> {
  let data = get_analysis_cache(params.mode).await?;

  let result = data.get_pp_history_for_rank(params.rank);

  Ok(Json(result))
}

pub async fn get_sanity_decay(
  Query(params): Query<SanityCheckQuery>,
) -> Result<Json<Vec<(NaiveDate, f32)>>, APIError> {
  let data = get_analysis_cache(params.mode).await?;

  let result = data.get_decay_history_for_rank(params.rank);

  Ok(Json(result))
}
