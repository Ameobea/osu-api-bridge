use std::collections::HashSet;

use serde::Serialize;
use tokio::sync::Semaphore;

use super::*;
use crate::{
  diffcalc::{self, CalculationRequest, ScoreInput, ScoreStatisticsInput},
  osu_api::Mod,
};

const MAX_PUBLIC_BATCH_SIZE: usize = 128;
const MAX_PUBLIC_COUNT: u32 = 10_000_000;

lazy_static::lazy_static! {
  // The sidecar has the authoritative CPU limit. This edge guard prevents public
  // callers from creating an unbounded number of queued bridge requests.
  static ref PUBLIC_SIMULATION_CONCURRENCY: Semaphore = Semaphore::new(16);
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

#[derive(Deserialize)]
pub(super) struct BatchSimulatePlayParams {
  beatmap_id: u64,
  params: Vec<SimulatePlayQueryParams>,
}

#[derive(Serialize)]
pub(super) struct BatchSimulatePlayResponse {
  pp: Vec<f64>,
}

fn bad_request(message: impl Into<String>) -> APIError {
  APIError {
    status: StatusCode::BAD_REQUEST,
    message: message.into(),
  }
}

fn parse_beatmap_id(beatmap_id: u64) -> Result<i32, APIError> {
  i32::try_from(beatmap_id)
    .ok()
    .filter(|id| *id > 0)
    .ok_or_else(|| bad_request("beatmap_id must be a positive 32-bit integer"))
}

fn parse_mods(mod_string: Option<&str>) -> Result<Vec<Mod>, APIError> {
  let mod_string = mod_string.unwrap_or_default().trim().to_ascii_uppercase();
  if mod_string.len() > 32
    || mod_string.len() % 2 != 0
    || !mod_string.bytes().all(|byte| byte.is_ascii_alphanumeric())
  {
    return Err(bad_request(
      "mods must contain at most 16 two-character acronyms",
    ));
  }

  let mut seen = HashSet::new();
  mod_string
    .as_bytes()
    .chunks_exact(2)
    .map(|chunk| {
      let acronym = std::str::from_utf8(chunk)
        .expect("validated ASCII")
        .to_owned();
      if !seen.insert(acronym.clone()) {
        return Err(bad_request(format!("duplicate mod acronym: {acronym}")));
      }
      Ok(Mod {
        acronym,
        settings: None,
      })
    })
    .collect()
}

fn checked_count(name: &str, value: Option<u32>) -> Result<Option<u32>, APIError> {
  if value > Some(MAX_PUBLIC_COUNT) {
    return Err(bad_request(format!("{name} exceeds {MAX_PUBLIC_COUNT}")));
  }
  Ok(value)
}

fn to_calculation(
  beatmap_id: i32,
  params: SimulatePlayQueryParams,
  request_id: Option<String>,
) -> Result<CalculationRequest, APIError> {
  let acc = params.acc.unwrap_or(100.0);
  if !acc.is_finite() || !(0.0..=100.0).contains(&acc) {
    return Err(bad_request("acc must be finite and in [0, 100]"));
  }

  let max_combo = checked_count("max_combo", params.max_combo)?;
  let misses = checked_count("misses", params.misses)?.unwrap_or(0);
  let n300 = checked_count("n300", params.n300)?;
  let n100 = checked_count("n100", params.n100)?;
  let n50 = checked_count("n50", params.n50)?;
  let supplied_counts = [n300, n100, n50];
  if supplied_counts.iter().any(Option::is_some) && supplied_counts.iter().any(Option::is_none) {
    return Err(bad_request("n300, n100, and n50 must be supplied together"));
  }

  Ok(CalculationRequest {
    request_id,
    beatmap_id,
    mods: parse_mods(params.mods.as_deref())?,
    is_classic: params.is_classic.unwrap_or(true),
    score: Some(ScoreInput {
      accuracy: acc,
      max_combo,
      legacy_total_score: None,
      statistics: Some(ScoreStatisticsInput {
        great: n300,
        ok: n100,
        meh: n50,
        miss: misses,
        large_tick_hit: None,
        large_tick_miss: None,
        small_tick_hit: None,
        small_tick_miss: None,
        slider_tail_hit: None,
        large_bonus: None,
        small_bonus: None,
      }),
    }),
  })
}

fn result_pp(result: diffcalc::CalculationResult) -> Result<f64, APIError> {
  if let Some(err) = result.error {
    warn!(
      "diffcalc rejected beatmap {} request {:?}: {}: {}",
      result.beatmap_id, result.request_id, err.code, err.message
    );
    let status = match err.code.as_str() {
      "beatmap_not_found" => StatusCode::NOT_FOUND,
      "invalid_calculation" => StatusCode::BAD_REQUEST,
      "calculation_timeout" => StatusCode::GATEWAY_TIMEOUT,
      _ => StatusCode::BAD_GATEWAY,
    };
    return Err(APIError {
      status,
      message: err.message,
    });
  }

  result
    .performance
    .map(|performance| performance.pp)
    .ok_or_else(|| APIError {
      status: StatusCode::BAD_GATEWAY,
      message: "Difficulty calculation service omitted performance attributes".to_owned(),
    })
}

fn acquire_public_permit() -> Result<tokio::sync::SemaphorePermit<'static>, APIError> {
  PUBLIC_SIMULATION_CONCURRENCY.try_acquire().map_err(|_| {
    crate::metrics::http_server::simulation_rejections_total("concurrency").inc();
    APIError {
      status: StatusCode::TOO_MANY_REQUESTS,
      message: "Too many concurrent simulation requests".to_owned(),
    }
  })
}

pub(super) async fn simulate_play_route(
  Path(beatmap_id): Path<u64>,
  Query(params): Query<SimulatePlayQueryParams>,
) -> Result<Json<SimulatePlayResponse>, APIError> {
  let _permit = acquire_public_permit()?;
  let request = to_calculation(parse_beatmap_id(beatmap_id)?, params, None)?;
  let mut results =
    diffcalc::calculate_all(diffcalc::Operation::SimulateSingle, vec![request]).await?;
  let result = results.pop().ok_or_else(|| APIError {
    status: StatusCode::BAD_GATEWAY,
    message: "Difficulty calculation service returned no result".to_owned(),
  })?;
  Ok(Json(SimulatePlayResponse {
    pp: result_pp(result)?,
  }))
}

pub(super) async fn batch_simulate_play_route(
  Path(path_beatmap_id): Path<u64>,
  body: String,
) -> Result<Json<BatchSimulatePlayResponse>, APIError> {
  let _permit = acquire_public_permit()?;
  let BatchSimulatePlayParams { beatmap_id, params } = serde_json::from_str(&body)
    .map_err(|err| bad_request(format!("Error parsing request body: {err}")))?;

  if beatmap_id != path_beatmap_id {
    return Err(bad_request("body beatmap_id must match the URL"));
  }
  if params.len() > MAX_PUBLIC_BATCH_SIZE {
    crate::metrics::http_server::simulation_rejections_total("batch_size").inc();
    return Err(bad_request(format!(
      "params exceeds the maximum batch size of {MAX_PUBLIC_BATCH_SIZE}"
    )));
  }

  let beatmap_id = parse_beatmap_id(beatmap_id)?;
  let requests = params
    .into_iter()
    .enumerate()
    .map(|(index, params)| to_calculation(beatmap_id, params, Some(index.to_string())))
    .collect::<Result<Vec<_>, _>>()?;
  let results = diffcalc::calculate_all(diffcalc::Operation::SimulateBatch, requests).await?;
  let pp = results
    .into_iter()
    .map(result_pp)
    .collect::<Result<Vec<_>, _>>()?;
  Ok(Json(BatchSimulatePlayResponse { pp }))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_compact_mod_strings() {
    let mods = parse_mods(Some("hddt")).unwrap();
    assert_eq!(
      mods.iter().map(|m| m.acronym.as_str()).collect::<Vec<_>>(),
      ["HD", "DT"]
    );
  }

  #[test]
  fn rejects_ambiguous_partial_hit_counts() {
    let result = to_calculation(
      75,
      SimulatePlayQueryParams {
        mods: None,
        is_classic: None,
        max_combo: None,
        acc: Some(98.0),
        misses: None,
        n300: None,
        n100: Some(1),
        n50: None,
      },
      None,
    );
    assert!(result.is_err());
  }
}
