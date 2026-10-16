use std::{
  io::Cursor,
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

use image::{codecs::png::PngEncoder, ExtendedColorType, ImageEncoder, RgbImage};
use lazy_static::lazy_static;
use pco::{standalone::simple_compress, ChunkConfig, DeltaSpec, ModeSpec};
use plotters::prelude::*;
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
  pub rank_to_pp: Vec<(u32, f32)>,
  /// Largest bucket before pp drops below `MIN_RELIABLE_PP`; ranks past it are tail noise for
  /// this mode and shouldn't be selectable.
  pub max_rank: u32,
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
  pub user: u32,
  pub ts: i64,
  pub rank: u32,
  pub pp: f32,
}

/// Fits a line to the (rank, pp) samples within a bucket and returns its value at `target_rank`,
/// falling back to the mean when a line can't be fit.  Ranks are centered on the target so the
/// intercept is the answer directly and the sums stay well-conditioned at high ranks.
fn estimate_bucket_center_pp(samples: &[RankSampleWithDate], target_rank: u32) -> f32 {
  let n = samples.len() as f64;
  let mean_pp = samples.iter().map(|s| s.pp as f64).sum::<f64>() / n;
  if samples.iter().all(|s| s.rank == samples[0].rank) {
    return mean_pp as f32;
  }

  let (mut sx, mut sxx, mut sxy) = (0f64, 0f64, 0f64);
  for s in samples {
    let x = s.rank as f64 - target_rank as f64;
    sx += x;
    sxx += x * x;
    sxy += x * (s.pp as f64 - mean_pp);
  }
  let den = n * sxx - sx * sx;
  if den.abs() < 1e-9 {
    return mean_pp as f32;
  }
  let slope = n * sxy / den;
  let fitted = mean_pp - slope * sx / n;
  if fitted > 0. {
    fitted as f32
  } else {
    mean_pp as f32
  }
}

/// On days with fewer samples than this in a bucket (the sparse 2014-2016 era), the bucket is
/// widened by these multiples of its normal half-width so the fit isn't driven by one or two
/// points.
const MIN_BUCKET_SAMPLES: usize = 6;
const BUCKET_WIDEN_FACTORS: [f32; 2] = [2.5, 5.];

/// Compute a sparse snapshot from raw samples without any interpolation.
/// Missing rank buckets are left as None.
fn compute_sparse_snapshot(
  samples: &[RankSampleWithDate],
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

    let mut slice = &samples[start_ix..end_ix];
    if slice.len() < MIN_BUCKET_SAMPLES && window > 0 {
      for &factor in &BUCKET_WIDEN_FACTORS {
        let wide = ((window as f32 * factor) as u32).max(1);
        let lo = target_rank.saturating_sub(wide);
        let hi = target_rank.saturating_add(wide);
        let s = samples.partition_point(|x| x.rank < lo);
        let e = samples.partition_point(|x| x.rank <= hi);
        if e - s > slice.len() {
          slice = &samples[s..e];
        }
        if slice.len() >= MIN_BUCKET_SAMPLES {
          break;
        }
      }
    }
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
    "SELECT DATE(timestamp) as date, CAST(`user` AS UNSIGNED) as user, \
     CAST(UNIX_TIMESTAMP(timestamp) AS SIGNED) as ts, CAST(pp_rank as UNSIGNED) as rank, pp_raw \
     as pp FROM updates WHERE mode = ? AND pp_rank > 0 AND pp_raw > 0",
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

/// A day is a shock when the median day-over-day log change of pp across the mid-ladder buckets
/// exceeds this.  Normal drift is ~0.01-0.1%/day; pp reworks and inactive purges move the whole
/// ladder 0.3-4% in a day.
const SHOCK_THRESHOLD: f64 = 0.003;
const SHOCK_MIN_RANK: u32 = 1_000;
const SHOCK_MAX_RANK: u32 = 500_000;
/// Shock days this close together are one event (a rollout with quiet days in between); the quiet
/// days have windows too short to estimate anything from anyway.
const SHOCK_MERGE_GAP_DAYS: usize = 3;

/// Inclusive `[start, end]` day index ranges of consecutive shock days.  Recalculations that roll
/// out over several days become a single interval.
fn detect_shock_intervals(
  pp_matrix: &[f32],
  buckets: &[u32],
  num_days: usize,
) -> Vec<(usize, usize)> {
  let num_buckets = buckets.len();
  let mid: Vec<usize> = (0..num_buckets)
    .filter(|&b| (SHOCK_MIN_RANK..=SHOCK_MAX_RANK).contains(&buckets[b]))
    .collect();

  let mut intervals: Vec<(usize, usize)> = Vec::new();
  let mut changes = Vec::with_capacity(mid.len());
  for day in 1..num_days {
    changes.clear();
    for &b in &mid {
      let prev = pp_matrix[(day - 1) * num_buckets + b];
      let cur = pp_matrix[day * num_buckets + b];
      if prev > 0. && cur > 0. {
        changes.push((cur as f64 / prev as f64).ln());
      }
    }
    if changes.len() < 10 {
      continue;
    }
    changes.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = changes.len();
    let median = if n % 2 == 0 {
      (changes[n / 2 - 1] + changes[n / 2]) / 2.
    } else {
      changes[n / 2]
    };
    if median.abs() <= SHOCK_THRESHOLD {
      continue;
    }
    match intervals.last_mut() {
      Some((_, end)) if day <= *end + 1 + SHOCK_MERGE_GAP_DAYS => *end = day,
      _ => intervals.push((day, day)),
    }
  }
  intervals
}

/// `[start, end]` window of `radius` days around `day` that never crosses a shock interval, or
/// `None` when `day` is inside one.  The last day of an interval is already fully post-shock, so it
/// is where windows for the days after it begin.
fn clamped_window(
  day: usize,
  radius: usize,
  num_days: usize,
  intervals: &[(usize, usize)],
) -> Option<(usize, usize)> {
  let mut start = day.saturating_sub(radius);
  let mut end = (day + radius).min(num_days - 1);
  for &(a, b) in intervals {
    if a <= day && day <= b {
      return None;
    }
    if b < day {
      start = start.max(b);
    }
    if a > day {
      end = end.min(a - 1);
      break;
    }
  }
  Some((start, end))
}

pub async fn rebuild_analysis_data(mode: Ruleset) -> Result<AnalysisData, APIError> {
  let mut samples = fetch_rank_samples(mode).await.map_err(|e| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Failed to fetch samples: {}", e.message),
  })?;
  if samples.is_empty() {
    return Err(APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "No rank samples available for analysis".to_owned(),
    });
  }

  let first_date = samples.iter().map(|s| s.date).min().unwrap();
  let last_date = samples.iter().map(|s| s.date).max().unwrap();
  let num_days = (last_date - first_date).num_days() as usize + 1;
  let all_dates: Vec<NaiveDate> = (0..num_days)
    .map(|i| first_date + chrono::Duration::days(i as i64))
    .collect();

  let buckets = target_ranks().to_vec();
  let num_buckets = buckets.len();

  samples.sort_unstable_by_key(|s| (s.date, s.rank));
  let mut sparse_snapshots: Vec<SparseSnapshot> = all_dates
    .iter()
    .map(|&date| SparseSnapshot {
      date,
      mode,
      bucket_values: vec![None; num_buckets],
      rows_scanned: 0,
      buckets_from_samples: 0,
    })
    .collect();
  let mut start = 0;
  while start < samples.len() {
    let date = samples[start].date;
    let end = start + samples[start..].partition_point(|s| s.date == date);
    if let Some(snapshot) = compute_sparse_snapshot(&samples[start..end], mode, date) {
      sparse_snapshots[(date - first_date).num_days() as usize] = snapshot;
    }
    start = end;
  }

  let interpolated = interpolate_snapshots_across_time(&sparse_snapshots);
  drop(sparse_snapshots);
  let pp_matrix: Vec<f32> = interpolated
    .iter()
    .flat_map(|s| s.bucket_values.iter().map(|v| v.unwrap_or(0.)))
    .collect();
  drop(interpolated);

  let shock_intervals = detect_shock_intervals(&pp_matrix, &buckets, num_days);
  info!(
    "{mode:?}: {} shock intervals: {:?}",
    shock_intervals.len(),
    shock_intervals
      .iter()
      .map(|&(a, b)| (all_dates[a], all_dates[b]))
      .collect::<Vec<_>>()
  );

  let smoothed_pp_matrix = smooth_matrix(&pp_matrix, num_buckets, num_days, &shock_intervals, 7);
  drop(pp_matrix);
  let surface_decay = calculate_surface_decay(
    &smoothed_pp_matrix,
    &buckets,
    num_days,
    &shock_intervals,
    24,
  );

  samples.sort_unstable_by_key(|s| (s.user, s.ts));
  let mut pairs = build_same_pp_pairs(&samples, first_date, num_days, &shock_intervals);
  drop(samples);
  info!("{mode:?}: {} same-pp update pairs", pairs.len());
  let (direct_decay, direct_counts) = calculate_direct_decay(&mut pairs, &buckets, num_days);
  drop(pairs);
  let (direct_decay, direct_counts) = smooth_direct_decay(
    &direct_decay,
    &direct_counts,
    num_buckets,
    num_days,
    &shock_intervals,
    7,
  );
  let decay_matrix = blend_decay(&surface_decay, &direct_decay, &direct_counts);

  let simulation_profile =
    generate_simulation_profile(&smoothed_pp_matrix, &decay_matrix, &buckets, num_days);

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

/// Triangular-weighted smoothing along time, never reaching across a shock interval.  Days inside
/// an interval keep their raw values.
fn smooth_matrix(
  pp_matrix: &[f32],
  num_buckets: usize,
  num_days: usize,
  shock_intervals: &[(usize, usize)],
  window_radius: usize,
) -> Vec<f32> {
  let mut smoothed = pp_matrix.to_vec();

  for i in 0..num_days {
    let Some((start_idx, end_idx)) = clamped_window(i, window_radius, num_days, shock_intervals)
    else {
      continue;
    };

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

    // apply lighter smoothing towards the most recent day to make the data more responsive
    let days_from_end = num_days - 1 - i;
    let recent_factor = if days_from_end < 5 {
      (1. - (days_from_end as f32 / 5.)) * 0.85
    } else {
      0.
    };

    for bucket in 0..num_buckets {
      let mut weighted_sum = 0.;
      for (j, &weight) in weights.iter().enumerate() {
        let day = start_idx + j;
        let val = pp_matrix[day * num_buckets + bucket];
        weighted_sum += val * weight;
      }
      let smoothed_val = weighted_sum / weight_sum;
      smoothed[i * num_buckets + bucket] =
        smoothed_val * (1. - recent_factor) + pp_matrix[i * num_buckets + bucket] * recent_factor;
    }
  }

  smoothed
}

fn linear_slope(points: &[(f32, f32)]) -> f32 {
  let n = points.len() as f64;
  let mean_x = points.iter().map(|p| p.0 as f64).sum::<f64>() / n;
  let mean_y = points.iter().map(|p| p.1 as f64).sum::<f64>() / n;
  let (mut sxx, mut sxy) = (0f64, 0f64);
  for &(x, y) in points {
    let dx = x as f64 - mean_x;
    sxx += dx * dx;
    sxy += dx * (y as f64 - mean_y);
  }
  if sxx < 1e-9 {
    0.
  } else {
    (sxy / sxx) as f32
  }
}

/// Decay derived from the pp surface: for each (day, bucket), the slope of the rank that the
/// bucket's pp maps to across the surrounding days.  NaN where the window is inside or too close to
/// a shock interval.
fn calculate_surface_decay(
  pp_matrix: &[f32],
  buckets: &[u32],
  num_days: usize,
  shock_intervals: &[(usize, usize)],
  window_radius: usize,
) -> Vec<f32> {
  let num_buckets = buckets.len();
  let mut decay_matrix = vec![f32::NAN; pp_matrix.len()];
  let mut points = Vec::with_capacity(2 * window_radius + 1);

  for day in 0..num_days {
    let Some((start_idx, end_idx)) = clamped_window(day, window_radius, num_days, shock_intervals)
    else {
      continue;
    };
    if end_idx < start_idx + 3 {
      continue;
    }

    for bucket_idx in 0..num_buckets {
      let current_pp = pp_matrix[day * num_buckets + bucket_idx];
      points.clear();
      for d in start_idx..=end_idx {
        let pp_row = &pp_matrix[d * num_buckets..(d + 1) * num_buckets];
        points.push((d as f32, get_rank_for_pp(pp_row, buckets, current_pp)));
      }

      let mut slope = linear_slope(&points);
      if bucket_idx == 0 && slope < 0. {
        slope = 0.;
      }
      decay_matrix[day * num_buckets + bucket_idx] = slope;
    }
  }

  decay_matrix
}

struct SamePpPair {
  rank: u32,
  start_day: u32,
  end_day: u32,
  /// ranks lost per day over the pair's span
  rate: f32,
}

const PAIR_MIN_DAYS: f32 = 1.;
const PAIR_MAX_DAYS: f32 = 45.;

/// Consecutive updates of one user whose pp didn't change measure ladder drift directly: the rank
/// change over the gap is pure decay at that pp.  Pairs whose span touches a shock interval are
/// dropped so reworks and purges don't leak into the rate.  `samples` must be sorted by (user, ts).
fn build_same_pp_pairs(
  samples: &[RankSampleWithDate],
  first_date: NaiveDate,
  num_days: usize,
  shock_intervals: &[(usize, usize)],
) -> Vec<SamePpPair> {
  let mut shock_prefix = vec![0u32; num_days + 1];
  for day in 0..num_days {
    let is_shock = shock_intervals.iter().any(|&(a, b)| a <= day && day <= b);
    shock_prefix[day + 1] = shock_prefix[day] + is_shock as u32;
  }

  let mut pairs = Vec::new();
  for w in samples.windows(2) {
    let (a, b) = (&w[0], &w[1]);
    if a.user != b.user || (a.pp - b.pp).abs() >= 0.005 {
      continue;
    }
    let span_days = (b.ts - a.ts) as f32 / 86_400.;
    if !(PAIR_MIN_DAYS..=PAIR_MAX_DAYS).contains(&span_days) {
      continue;
    }
    let start_day = (a.date - first_date).num_days() as usize;
    let end_day = (b.date - first_date).num_days() as usize;
    if shock_prefix[end_day + 1] - shock_prefix[start_day] > 0 {
      continue;
    }
    pairs.push(SamePpPair {
      rank: a.rank,
      start_day: start_day as u32,
      end_day: end_day as u32,
      rate: (b.rank as f32 - a.rank as f32) / span_days,
    });
  }
  pairs
}

/// Pairs within this fraction of a bucket's rank contribute to its direct decay estimate.
const DIRECT_RANK_WINDOW: f64 = 0.10;
const DIRECT_MIN_PAIRS: usize = 5;
/// Smoothed pair count at which the direct estimate fully replaces the surface-derived one.
const DIRECT_FULL_WEIGHT_PAIRS: f32 = 15.;

/// Per (day, bucket) median rate over same-pp pairs near the bucket's rank whose span covers the
/// day.  Returns (values, counts); value is NaN where fewer than `DIRECT_MIN_PAIRS` pairs cover it.
fn calculate_direct_decay(
  pairs: &mut [SamePpPair],
  buckets: &[u32],
  num_days: usize,
) -> (Vec<f32>, Vec<f32>) {
  pairs.sort_unstable_by_key(|p| p.rank);
  let num_buckets = buckets.len();
  let mut values = vec![f32::NAN; num_days * num_buckets];
  let mut counts = vec![0f32; num_days * num_buckets];
  let mut entries: Vec<(u32, f32)> = Vec::new();

  for (bucket_idx, &rank) in buckets.iter().enumerate() {
    let lo = rank as f64 * (1. - DIRECT_RANK_WINDOW);
    let hi = rank as f64 * (1. + DIRECT_RANK_WINDOW);
    let lo_ix = pairs.partition_point(|p| (p.rank as f64) < lo);
    let hi_ix = pairs.partition_point(|p| (p.rank as f64) <= hi);

    entries.clear();
    for p in &pairs[lo_ix..hi_ix] {
      for day in p.start_day..=p.end_day {
        entries.push((day, p.rate));
      }
    }
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));

    let mut i = 0;
    while i < entries.len() {
      let day = entries[i].0;
      let mut j = i;
      while j < entries.len() && entries[j].0 == day {
        j += 1;
      }
      let n = j - i;
      if n >= DIRECT_MIN_PAIRS {
        let ix = day as usize * num_buckets + bucket_idx;
        values[ix] = entries[i + (n - 1) / 2].1;
        counts[ix] = n as f32;
      }
      i = j;
    }
  }

  (values, counts)
}

/// Same triangular smoothing as `smooth_matrix`, skipping NaN cells and carrying the pair counts
/// along so the blend can weight by evidence.
fn smooth_direct_decay(
  values: &[f32],
  counts: &[f32],
  num_buckets: usize,
  num_days: usize,
  shock_intervals: &[(usize, usize)],
  window_radius: usize,
) -> (Vec<f32>, Vec<f32>) {
  let mut smoothed_values = vec![f32::NAN; values.len()];
  let mut smoothed_counts = vec![0f32; counts.len()];

  for day in 0..num_days {
    let Some((start, end)) = clamped_window(day, window_radius, num_days, shock_intervals) else {
      continue;
    };
    let window_len = (end - start + 1) as f32;

    for bucket_idx in 0..num_buckets {
      let (mut weight_all, mut weight_valid, mut value_sum, mut count_sum) =
        (0f32, 0f32, 0f32, 0f32);
      for d in start..=end {
        let weight = 1. - (d as f32 - day as f32).abs() / window_len;
        let ix = d * num_buckets + bucket_idx;
        weight_all += weight;
        count_sum += weight * counts[ix];
        if values[ix].is_finite() {
          weight_valid += weight;
          value_sum += weight * values[ix];
        }
      }
      let ix = day * num_buckets + bucket_idx;
      if weight_valid > 0. {
        smoothed_values[ix] = value_sum / weight_valid;
      }
      smoothed_counts[ix] = count_sum / weight_all;
    }
  }

  (smoothed_values, smoothed_counts)
}

fn blend_decay(surface: &[f32], direct: &[f32], direct_counts: &[f32]) -> Vec<f32> {
  surface
    .iter()
    .zip(direct)
    .zip(direct_counts)
    .map(|((&s, &d), &count)| {
      if !d.is_finite() {
        return s;
      }
      if !s.is_finite() {
        return d;
      }
      let w = (count / DIRECT_FULL_WEIGHT_PAIRS).min(1.);
      w * d + (1. - w) * s
    })
    .collect()
}

fn get_rank_for_pp(pp_row: &[f32], buckets: &[u32], target_pp: f32) -> f32 {
  let num_buckets = buckets.len();
  // pp_row is descending (Rank 1 has high PP)
  // Find first index where PP < target_pp
  let idx = pp_row.partition_point(|&pp| pp > target_pp);

  if idx == 0 {
    1.
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

fn smooth_simulation_curve(
  data: &[(u32, f32)],
  min_bandwidth: f32,
  max_bandwidth: f32,
  bandwidth_scale: f32,
) -> Vec<(u32, f32)> {
  // filtering is handled in log/log space
  let valid_points: Vec<(usize, f32, f32)> = data
    .iter()
    .enumerate()
    .filter(|(_, (_, val))| *val > 1e-9)
    .map(|(i, (rank, val))| (i, (*rank as f32).ln(), val.ln()))
    .collect();

  if valid_points.len() < 2 {
    return data.to_vec();
  }

  let points_log: Vec<(f32, f32)> = valid_points.iter().map(|&(_, x, y)| (x, y)).collect();
  let mut smoothed_values = Vec::with_capacity(data.len());

  let kernel = |dist: f32, h: f32| (-0.5 * (dist / h).powi(2)).exp();

  let mut initial_smooth = Vec::with_capacity(valid_points.len());
  for &(x, _) in &points_log {
    // adaptive bandwidth, applying stronger smoothing further down on the leaderboard
    let h = min_bandwidth + (max_bandwidth - min_bandwidth) * (x / bandwidth_scale).min(1.);

    let mut sum_wy = 0.;
    let mut sum_w = 0.;

    for &(px, py) in &points_log {
      let dist = (x - px).abs();
      if dist > h * 4. {
        continue;
      }
      let w = kernel(dist, h);
      sum_wy += w * py;
      sum_w += w;
    }

    if sum_w > 1e-9 {
      initial_smooth.push(sum_wy / sum_w);
    } else {
      initial_smooth.push(0.);
    }
  }

  let mut residuals: Vec<f32> = valid_points
    .iter()
    .zip(initial_smooth.iter())
    .map(|((_, _, y), smooth_y)| (y - smooth_y).abs())
    .collect();

  residuals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
  let median_res = residuals[residuals.len() / 2];

  let robust_weights: Vec<f32> = valid_points
    .iter()
    .zip(initial_smooth.iter())
    .map(|((_, _, y), smooth_y)| {
      let r = (y - smooth_y).abs();
      let s = 6. * median_res;
      if s < 1e-9 {
        1.
      } else {
        let u = (r / s).min(1.);
        (1. - u * u).powi(2)
      }
    })
    .collect();

  for (rank, _) in data {
    let x = (*rank as f32).ln();
    let h = min_bandwidth + (max_bandwidth - min_bandwidth) * (x / bandwidth_scale).min(1.);

    let mut sum_wy = 0.;
    let mut sum_w = 0.;

    for (i, &(px, py)) in points_log.iter().enumerate() {
      let dist = (x - px).abs();
      if dist > h * 4. {
        continue;
      }
      let w = kernel(dist, h) * robust_weights[i];
      sum_wy += w * py;
      sum_w += w;
    }

    if sum_w > 1e-9 {
      smoothed_values.push((*rank, (sum_wy / sum_w).exp()));
    } else {
      let nearest = points_log
        .iter()
        .min_by(|a, b| (a.0 - x).abs().partial_cmp(&(b.0 - x).abs()).unwrap())
        .map(|&(_, y)| y.exp())
        .unwrap_or(0.);
      smoothed_values.push((*rank, nearest));
    }
  }

  smoothed_values
}

/// Number of most recent valid decay rows averaged into the simulation's decay curve.
const SIM_DECAY_LOOKBACK_ROWS: usize = 14;
const MIN_RELIABLE_PP: f32 = 30.;

fn generate_simulation_profile(
  pp_matrix: &[f32],
  decay_matrix: &[f32],
  buckets: &[u32],
  num_days: usize,
) -> SimulationProfile {
  let num_buckets = buckets.len();
  let latest_pp_row = &pp_matrix[(num_days - 1) * num_buckets..];

  let rank_to_pp: Vec<(u32, f32)> = buckets
    .iter()
    .copied()
    .zip(latest_pp_row.iter().copied())
    .collect();
  let max_rank = buckets
    .iter()
    .zip(latest_pp_row)
    .take_while(|(_, &pp)| pp >= MIN_RELIABLE_PP)
    .map(|(&rank, _)| rank)
    .last()
    .unwrap_or(buckets[0]);

  // The decay matrix already respects shock intervals, so its most recent valid rows are the
  // cleanest view of the current rate.
  let mut rank_to_decay = Vec::with_capacity(num_buckets);
  for bucket_idx in 0..num_buckets {
    let mut sum = 0.;
    let mut n = 0;
    for day in (0..num_days).rev() {
      let val = decay_matrix[day * num_buckets + bucket_idx];
      if val.is_finite() {
        sum += val;
        n += 1;
        if n == SIM_DECAY_LOOKBACK_ROWS {
          break;
        }
      }
    }
    let decay = if n > 0 { sum / n as f32 } else { 0. };
    rank_to_decay.push((buckets[bucket_idx], decay));
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

  let min_bandwidth = 0.125;
  let max_bandwidth = 0.35;
  // Determines the log-rank value at which the bandwidth transitions from minimum to maximum
  let bandwidth_scale = 10.;

  let rank_to_decay = smooth_simulation_curve(
    &rank_to_decay,
    min_bandwidth,
    max_bandwidth,
    bandwidth_scale,
  );
  let rank_to_density = smooth_simulation_curve(
    &rank_to_density,
    min_bandwidth,
    max_bandwidth,
    bandwidth_scale,
  );
  let rank_to_pp = smooth_simulation_curve(&rank_to_pp, 0.001, 0.05, 10.);
  SimulationProfile {
    rank_to_decay,
    rank_to_density,
    rank_to_pp,
    max_rank,
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

/// Encodes NaN decay cells (shock intervals) in the binary dataset; the client maps it back to NaN.
const DECAY_NAN_SENTINEL: i32 = i32::MIN;

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
      .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp())
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
      let val = if val.is_finite() {
        (val * 100.) as i32
      } else {
        DECAY_NAN_SENTINEL
      };
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

pub async fn get_analysis_plot(Query(params): Query<AnalysisQuery>) -> Result<Response, APIError> {
  let data = get_analysis_cache(params.mode).await?;
  let profile = &data.simulation_profile;

  let width: u32 = 1024;
  let height: u32 = 1200;

  let mut buffer = vec![0u8; (width as usize) * (height as usize) * 3];

  {
    let root = BitMapBackend::with_buffer(&mut buffer, (width, height)).into_drawing_area();
    root.fill(&WHITE).map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Failed to fill background: {err}"),
    })?;

    let areas = root.split_evenly((3, 1));

    let num_days = data.dates.len();
    let num_buckets = data.buckets.len();
    let mut rank_to_pp = Vec::with_capacity(num_buckets);
    for bucket_idx in 0..num_buckets {
      let rank = data.buckets[bucket_idx];
      let latest_pp = data.pp_matrix[(num_days - 1) * num_buckets + bucket_idx];
      rank_to_pp.push((rank, latest_pp));
    }

    let mut chart_pp = ChartBuilder::on(&areas[0])
      .caption("Rank vs PP", ("sans-serif", 30))
      .margin(10)
      .x_label_area_size(40)
      .y_label_area_size(60)
      .build_cartesian_2d(
        (1f32..3_000_000f32).log_scale(),
        (1f32..50_000f32).log_scale(),
      )
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to build chart: {}", e),
      })?;

    chart_pp
      .configure_mesh()
      .x_desc("Rank")
      .y_desc("PP")
      .draw()
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw mesh: {}", e),
      })?;

    chart_pp
      .draw_series(LineSeries::new(
        rank_to_pp.iter().map(|(r, v)| (*r as f32, *v)),
        &RED,
      ))
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw series: {}", e),
      })?;

    let mut chart_decay = ChartBuilder::on(&areas[1])
      .caption("Rank vs Decay", ("sans-serif", 30))
      .margin(10)
      .x_label_area_size(40)
      .y_label_area_size(60)
      .build_cartesian_2d(
        (1f32..3_000_000f32).log_scale(),
        (1e-5f32..1000.0f32).log_scale(),
      )
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to build chart: {}", e),
      })?;

    chart_decay
      .configure_mesh()
      .x_desc("Rank")
      .y_desc("Decay")
      .draw()
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw mesh: {}", e),
      })?;

    chart_decay
      .draw_series(LineSeries::new(
        profile
          .rank_to_decay
          .iter()
          .map(|(r, v)| (*r as f32, *v))
          .filter(|(_, v)| *v > 0.),
        &BLUE,
      ))
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw series: {}", e),
      })?;

    let mut chart_density = ChartBuilder::on(&areas[2])
      .caption("Rank vs Density", ("sans-serif", 30))
      .margin(10)
      .x_label_area_size(40)
      .y_label_area_size(60)
      .build_cartesian_2d(
        (1f32..3_000_000f32).log_scale(),
        (0.0001f32..1_000_000f32).log_scale(),
      )
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to build chart: {}", e),
      })?;

    chart_density
      .configure_mesh()
      .x_desc("Rank")
      .y_desc("Density (Ranks per PP)")
      .draw()
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw mesh: {}", e),
      })?;

    chart_density
      .draw_series(LineSeries::new(
        profile
          .rank_to_density
          .iter()
          .map(|(r, v)| (*r as f32, *v))
          .filter(|(_, v)| *v > 0.),
        &GREEN,
      ))
      .map_err(|e| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to draw series: {}", e),
      })?;

    root.present().map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Failed to present bitmap: {err}"),
    })?;
  }

  let rgb_image = RgbImage::from_raw(width, height, buffer).ok_or_else(|| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: "Failed to create image from raw buffer".into(),
  })?;

  let mut png_bytes = Vec::new();
  {
    let mut cursor = Cursor::new(&mut png_bytes);
    let encoder = PngEncoder::new(&mut cursor);

    encoder
      .write_image(&rgb_image, width, height, ExtendedColorType::Rgb8)
      .map_err(|err| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Failed to encode PNG: {err}"),
      })?;
  }

  Ok(([(header::CONTENT_TYPE, "image/png")], png_bytes).into_response())
}
