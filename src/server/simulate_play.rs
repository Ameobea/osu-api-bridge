use std::{io::Read, str::FromStr};

use rosu_pp::{any::DifficultyAttributes, Beatmap, Performance};
use rosu_v2::model::mods::GameModsLegacy;
use serde::Serialize;

use super::*;
use crate::db::db_pool;

async fn download_beatmap(beatmap_id: u64) -> Result<Beatmap, APIError> {
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
    return Err(APIError {
      status: StatusCode::NOT_FOUND,
      message: "Beatmap not found".to_string(),
    });
  };

  let mut decoder = flate2::read::GzDecoder::new(&beatmap[..]);
  let mut decompressed = Vec::new();
  decoder
    .read_to_end(&mut decompressed)
    .map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error decompressing beatmap: {err}"),
    })?;

  Beatmap::from_bytes(&decompressed).map_err(|err| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: format!("Error parsing beatmap: {err}"),
  })
}

fn compute_diff_attrs(
  beatmap: &Beatmap,
  mod_string: &str,
) -> Result<DifficultyAttributes, APIError> {
  let mods = GameModsLegacy::from_str(mod_string).map_err(|err| APIError {
    status: StatusCode::BAD_REQUEST,
    message: format!("Invalid mods: {err}"),
  })?;
  Ok(
    rosu_pp::Difficulty::new()
      .mods(mods.bits())
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

  let mut perf = Performance::new(diff_attrs).mods(mods.bits());
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
  let beatmap = download_beatmap(beatmap_id).await?;
  let diff_attrs = compute_diff_attrs(&beatmap, params.mods.as_deref().unwrap_or_default())?;
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

  let beatmap = download_beatmap(beatmap_id).await?;
  let Some(first_params) = params.first() else {
    return Ok(Json(BatchSimulatePlayResponse { pp: Vec::new() }));
  };
  let mut last_mods = first_params.mods.clone();
  let mut diff_attrs = compute_diff_attrs(&beatmap, last_mods.as_deref().unwrap_or_default())?;
  let mut pps = Vec::new();
  for params in params {
    if params.mods != last_mods {
      last_mods = params.mods.clone();
      diff_attrs = compute_diff_attrs(&beatmap, last_mods.as_deref().unwrap_or_default())?;
    }

    let pp = simulate_play(diff_attrs.clone(), &params).await?;
    pps.push(pp);
  }

  Ok(Json(BatchSimulatePlayResponse { pp: pps }))
}
