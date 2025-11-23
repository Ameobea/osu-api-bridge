use std::{
  io::{Read, Write},
  str::FromStr,
  sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
  },
  time::Duration,
};

use moka::sync::Cache;
use rosu_pp::{
  any::DifficultyAttributes,
  model::{
    beatmap::BreakPeriod,
    control_point::{DifficultyPoint, EffectPoint, TimingPoint},
    hit_object::{HitObject, HitSoundType},
  },
  Beatmap, Performance,
};
use rosu_v2::model::mods::GameModsLegacy;
use serde::Serialize;
use tokio::sync::Semaphore;

use super::*;
use crate::db::db_pool;

async fn compress_and_insert_beatmap(beatmap_id: i32, raw_beatmap: &[u8]) -> Result<(), APIError> {
  let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());

  encoder
    .write_all(raw_beatmap)
    .expect("Failed to write to encoder");
  let raw_beatmap_gzipped = encoder.finish().expect("Failed to finish encoder");

  sqlx::query!(
    "INSERT INTO fetched_beatmaps (beatmap_id, raw_beatmap_gzipped) VALUES (?, ?)",
    beatmap_id,
    raw_beatmap_gzipped
  )
  .execute(crate::db::db_pool())
  .await
  .map_err(|err| {
    error!("Failed to insert beatmap {beatmap_id}: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Failed to insert beatmap {beatmap_id}"),
    }
  })?;

  Ok(())
}

async fn download_beatmap(beatmap_id: i32) -> Result<Vec<u8>, APIError> {
  const MAX_RETRIES: u32 = 5;
  const BACKOFF_MS: u64 = 1000;

  let _permit = DOWNLOAD_SEMAPHORE.acquire().await.unwrap();

  for attempt in 0..MAX_RETRIES {
    let url = format!("https://osu.ppy.sh/osu/{beatmap_id}");
    info!("Downloading beatmap {beatmap_id} from osu...");

    let _timer =
      crate::metrics::http_server::beatmap_download_response_time_seconds().start_timer();

    let resp = reqwest::get(&url).await.map_err(|err| {
      error!(
        "Error fetching beatmap {beatmap_id} (attempt {}): {err}",
        attempt + 1
      );
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Error fetching beatmap {beatmap_id}"),
      }
    })?;

    if resp.status().is_success() {
      info!("Successfully downloaded beatmap {beatmap_id}");
      let raw_beatmap = resp.bytes().await.unwrap();
      return Ok(raw_beatmap.to_vec());
    } else if attempt < MAX_RETRIES - 1 {
      let status = resp.status();
      warn!(
        "Failed to fetch beatmap {beatmap_id} (attempt {}): {status}, retrying...",
        attempt + 1
      );
      tokio::time::sleep(Duration::from_millis(BACKOFF_MS * (attempt as u64 + 1))).await;
    } else {
      let status = resp.status();
      let body = resp
        .text()
        .await
        .unwrap_or_else(|_| "Failed to fetch body".to_owned());
      error!("Failed to fetch beatmap {beatmap_id} after {MAX_RETRIES} attempts: {status} {body}");
      return Err(APIError {
        status: StatusCode::NOT_FOUND,
        message: format!("Failed to fetch beatmap {beatmap_id}"),
      });
    }
  }

  unreachable!()
}

async fn download_and_store_beatmap(beatmap_id: u64) -> Result<Beatmap, APIError> {
  let raw_beatmap = download_beatmap(beatmap_id as _).await?;
  compress_and_insert_beatmap(beatmap_id as _, &raw_beatmap).await?;

  Beatmap::from_bytes(&raw_beatmap).map_err(|err| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Error parsing beatmap: {err}"),
  })
}

lazy_static::lazy_static! {
  static ref BEATMAP_CACHE_BYTES: AtomicI64 = AtomicI64::new(0);
  static ref BEATMAP_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
  static ref BEATMAP_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
  static ref BEATMAP_CACHE: Cache<u64, Arc<Beatmap>> = Cache::builder()
    .max_capacity(10_000)
    .eviction_listener(|_key: Arc<u64>, val: Arc<Beatmap>, _cause| {
      let bytes = estimate_beatmap_size(&val);
      let new_total = BEATMAP_CACHE_BYTES.fetch_sub(bytes, Ordering::Relaxed) - bytes;
      crate::metrics::http_server::beatmap_cache_bytes().set(new_total as u64);
    })
    .build();
  static ref DOWNLOAD_SEMAPHORE: Semaphore = Semaphore::new(1);
}

fn update_cache_hit_rate(hit: bool) {
  let (hits, misses) = if hit {
    (
      BEATMAP_CACHE_HITS.fetch_add(1, Ordering::Relaxed) + 1,
      BEATMAP_CACHE_MISSES.load(Ordering::Relaxed),
    )
  } else {
    (
      BEATMAP_CACHE_HITS.load(Ordering::Relaxed),
      BEATMAP_CACHE_MISSES.fetch_add(1, Ordering::Relaxed) + 1,
    )
  };

  let total = hits + misses;
  if total > 0 {
    let rate = (hits as f64 / total as f64) * 100.0;
    crate::metrics::http_server::beatmap_cache_hit_rate().set(rate);
  }
}

fn estimate_beatmap_size(beatmap: &Beatmap) -> i64 {
  let base_size = std::mem::size_of::<Beatmap>() as i64;

  let breaks_size = (beatmap.breaks.capacity() * std::mem::size_of::<BreakPeriod>()) as i64;
  let timing_points_size =
    (beatmap.timing_points.capacity() * std::mem::size_of::<TimingPoint>()) as i64;
  let difficulty_points_size =
    (beatmap.difficulty_points.capacity() * std::mem::size_of::<DifficultyPoint>()) as i64;
  let effect_points_size =
    (beatmap.effect_points.capacity() * std::mem::size_of::<EffectPoint>()) as i64;
  let hit_objects_size = (beatmap.hit_objects.capacity() * std::mem::size_of::<HitObject>()) as i64;
  let hit_sounds_size =
    (beatmap.hit_sounds.capacity() * std::mem::size_of::<HitSoundType>()) as i64;

  base_size
    + breaks_size
    + timing_points_size
    + difficulty_points_size
    + effect_points_size
    + hit_objects_size
    + hit_sounds_size
}

fn insert_beatmap_into_cache(beatmap_id: u64, mut beatmap: Beatmap) -> Arc<Beatmap> {
  beatmap.breaks.shrink_to_fit();
  beatmap.timing_points.shrink_to_fit();
  beatmap.difficulty_points.shrink_to_fit();
  beatmap.effect_points.shrink_to_fit();
  beatmap.hit_objects.shrink_to_fit();
  beatmap.hit_sounds.shrink_to_fit();

  let beatmap = Arc::new(beatmap);
  let bytes = estimate_beatmap_size(&beatmap);
  BEATMAP_CACHE.insert(beatmap_id, Arc::clone(&beatmap));
  let new_total = BEATMAP_CACHE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
  crate::metrics::http_server::beatmap_cache_bytes().set(new_total as u64);
  beatmap
}

fn decompress_and_parse_beatmap(
  beatmap_id: u64,
  raw_beatmap_gzipped: &[u8],
) -> Result<Arc<Beatmap>, APIError> {
  let mut decoder = flate2::read::GzDecoder::new(raw_beatmap_gzipped);
  let mut decompressed = Vec::new();
  decoder
    .read_to_end(&mut decompressed)
    .map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error decompressing beatmap {beatmap_id}: {err}"),
    })?;

  let beatmap = Beatmap::from_bytes(&decompressed).map_err(|err| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Error parsing beatmap {beatmap_id}: {err}"),
  })?;

  Ok(insert_beatmap_into_cache(beatmap_id, beatmap))
}

async fn fetch_beatmaps_from_db(beatmap_ids: &[u64]) -> Result<Vec<(i64, Vec<u8>)>, APIError> {
  let placeholders = beatmap_ids
    .iter()
    .map(|_| "?")
    .collect::<Vec<_>>()
    .join(",");
  let query_str = format!(
    "SELECT beatmap_id, raw_beatmap_gzipped FROM fetched_beatmaps WHERE beatmap_id IN ({})",
    placeholders
  );

  let mut query = sqlx::query_as::<_, (i64, Vec<u8>)>(&query_str);
  for &id in beatmap_ids {
    query = query.bind(id as i64);
  }

  query.fetch_all(db_pool()).await.map_err(|err| {
    error!("Error fetching beatmaps from DB: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error fetching beatmaps from DB: {err}"),
    }
  })
}

async fn fetch_beatmap(beatmap_id: u64) -> Result<Arc<Beatmap>, APIError> {
  if let Some(beatmap) = BEATMAP_CACHE.get(&beatmap_id) {
    update_cache_hit_rate(true);
    return Ok(Arc::clone(&beatmap));
  }
  update_cache_hit_rate(false);

  let beatmap = sqlx::query_scalar!(
    "SELECT raw_beatmap_gzipped FROM fetched_beatmaps WHERE beatmap_id = ?",
    beatmap_id
  )
  .fetch_optional(db_pool())
  .await
  .map_err(|err| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Error fetching beatmap: {err}"),
  })?;

  let Some(beatmap) = beatmap else {
    let beatmap = download_and_store_beatmap(beatmap_id).await?;
    return Ok(insert_beatmap_into_cache(beatmap_id, beatmap));
  };

  decompress_and_parse_beatmap(beatmap_id, &beatmap)
}

pub(super) async fn fetch_beatmaps_cached_only(beatmap_ids: &[u64]) -> Vec<Option<Arc<Beatmap>>> {
  let mut results: FxHashMap<u64, Option<Arc<Beatmap>>> = FxHashMap::default();
  let mut missing_ids = Vec::new();

  for &beatmap_id in beatmap_ids {
    if let Some(beatmap) = BEATMAP_CACHE.get(&beatmap_id) {
      update_cache_hit_rate(true);
      results.insert(beatmap_id, Some(Arc::clone(&beatmap)));
    } else {
      update_cache_hit_rate(false);
      missing_ids.push(beatmap_id);
    }
  }

  if missing_ids.is_empty() {
    return beatmap_ids
      .iter()
      .map(|id| results.get(id).unwrap().clone())
      .collect();
  }

  let db_results = fetch_beatmaps_from_db(&missing_ids)
    .await
    .unwrap_or_else(|_| Vec::new());

  for &beatmap_id in &missing_ids {
    if let Some((_, raw_beatmap_gzipped)) =
      db_results.iter().find(|(id, _)| *id == beatmap_id as i64)
    {
      match decompress_and_parse_beatmap(beatmap_id, raw_beatmap_gzipped) {
        Ok(beatmap) => {
          results.insert(beatmap_id, Some(beatmap));
        },
        Err(err) => {
          error!("Error processing beatmap {beatmap_id}: {}", err.message);
          results.insert(beatmap_id, None);
        },
      }
    } else {
      results.insert(beatmap_id, None);
    }
  }

  beatmap_ids
    .iter()
    .map(|id| results.get(id).and_then(|opt| opt.clone()))
    .collect()
}

pub(super) async fn fetch_and_store_beatmap(beatmap_id: u64) -> Result<Arc<Beatmap>, APIError> {
  if let Some(beatmap) = BEATMAP_CACHE.get(&beatmap_id) {
    update_cache_hit_rate(true);
    return Ok(Arc::clone(&beatmap));
  }
  update_cache_hit_rate(false);

  let db_result = sqlx::query_scalar!(
    "SELECT raw_beatmap_gzipped FROM fetched_beatmaps WHERE beatmap_id = ?",
    beatmap_id
  )
  .fetch_optional(db_pool())
  .await
  .map_err(|err| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Error fetching beatmap: {err}"),
  })?;

  if let Some(raw_beatmap_gzipped) = db_result {
    return decompress_and_parse_beatmap(beatmap_id, &raw_beatmap_gzipped);
  }

  let beatmap = download_and_store_beatmap(beatmap_id).await?;
  Ok(insert_beatmap_into_cache(beatmap_id, beatmap))
}

fn compute_diff_attrs(
  beatmap: &Beatmap,
  mod_string: &str,
  is_classic: bool,
) -> Result<DifficultyAttributes, APIError> {
  let mods = GameModsLegacy::from_str(mod_string).map_err(|err| APIError {
    status: StatusCode::BAD_REQUEST,
    message: format!("Invalid mods: {err}"),
  })?;
  Ok(
    rosu_pp::Difficulty::new()
      .mods(mods.bits())
      .lazer(!is_classic)
      .calculate(&beatmap),
  )
}

async fn simulate_play(
  diff_attrs: DifficultyAttributes,
  params: &SimulatePlayQueryParams,
) -> Result<f64, APIError> {
  let mods =
    GameModsLegacy::from_str(params.mods.as_deref().unwrap_or_default()).map_err(|err| {
      APIError {
        status: StatusCode::BAD_REQUEST,
        message: format!("Invalid mods: {err}"),
      }
    })?;

  let is_classic = params.is_classic.unwrap_or(true);
  let mut perf = Performance::new(diff_attrs)
    .mods(mods.bits())
    .lazer(!is_classic);
  if let Some(acc) = params.acc {
    perf = perf.accuracy(acc);
  }
  if let Some(max_combo) = params.max_combo {
    perf = perf.combo(max_combo);
  }
  if let Some(misses) = params.misses {
    perf = perf.misses(misses);
  }
  if let Some(n300) = params.n300 {
    perf = perf.n300(n300);
  }
  if let Some(n100) = params.n100 {
    perf = perf.n100(n100);
  }
  if let Some(n50) = params.n50 {
    perf = perf.n50(n50);
  }

  Ok(perf.calculate().pp())
}

#[derive(Deserialize)]
pub(super) struct SimulatePlayQueryParams {
  mods: Option<String>,
  is_classic: Option<bool>,
  max_combo: Option<u32>,
  acc: Option<f64>,
  misses: Option<u32>,
  n300: Option<u32>,
  n100: Option<u32>,
  n50: Option<u32>,
}

#[derive(Serialize)]
pub(super) struct SimulatePlayResponse {
  pp: f64,
}

pub(super) async fn simulate_play_route(
  Path(beatmap_id): Path<u64>,
  Query(params): Query<SimulatePlayQueryParams>,
) -> Result<Json<SimulatePlayResponse>, APIError> {
  let beatmap = fetch_beatmap(beatmap_id).await?;
  let is_classic = params.is_classic.unwrap_or(true);
  let diff_attrs = compute_diff_attrs(
    &beatmap,
    params.mods.as_deref().unwrap_or_default(),
    is_classic,
  )?;
  simulate_play(diff_attrs, &params)
    .await
    .map(|pp| Json(SimulatePlayResponse { pp }))
}

#[derive(Deserialize)]
pub(super) struct BatchSimulatePlayParams {
  beatmap_id: u64,
  params: Vec<SimulatePlayQueryParams>,
}

#[derive(Serialize)]
pub(super) struct BatchSimulatePlayResponse {
  pp: Vec<f64>,
}

pub(super) async fn batch_simulate_play_route(
  body: String,
) -> Result<Json<BatchSimulatePlayResponse>, APIError> {
  let BatchSimulatePlayParams { beatmap_id, params } =
    serde_json::from_str(&body).map_err(|err| APIError {
      status: StatusCode::BAD_REQUEST,
      message: format!("Error parsing request body: {err}"),
    })?;

  let beatmap = fetch_beatmap(beatmap_id).await?;
  let Some(first_params) = params.first() else {
    return Ok(Json(BatchSimulatePlayResponse { pp: Vec::new() }));
  };
  let mut last_mods = first_params.mods.clone();
  let is_classic = first_params.is_classic.unwrap_or(true);
  let mut diff_attrs = compute_diff_attrs(
    &beatmap,
    last_mods.as_deref().unwrap_or_default(),
    is_classic,
  )?;
  let mut pps = Vec::new();
  for params in params {
    if params.mods != last_mods {
      last_mods = params.mods.clone();
      let is_classic = params.is_classic.unwrap_or(true);
      diff_attrs = compute_diff_attrs(
        &beatmap,
        last_mods.as_deref().unwrap_or_default(),
        is_classic,
      )?;
    }

    let pp = simulate_play(diff_attrs.clone(), &params).await?;
    pps.push(pp);
  }

  Ok(Json(BatchSimulatePlayResponse { pp: pps }))
}
