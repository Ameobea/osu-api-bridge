use std::time::{Duration, Instant};

use axum::http::StatusCode;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::{metrics::http_server, osu_api::Mod, server::APIError, settings::DiffcalcSettings};

static DIFFCALC: OnceCell<DiffcalcClient> = OnceCell::const_new();

pub fn init(settings: &DiffcalcSettings) {
  assert!(
    settings.api_key.len() >= 32,
    "diffcalc.api_key must contain at least 32 characters"
  );
  let client = Client::builder()
    .connect_timeout(Duration::from_secs(3))
    .pool_idle_timeout(Duration::from_secs(90))
    .build()
    .expect("failed to construct diffcalc HTTP client");

  DIFFCALC
    .set(DiffcalcClient {
      client,
      url: settings.url.trim_end_matches('/').to_owned(),
      api_key: settings.api_key.clone(),
      max_batch_size: settings.max_batch_size.clamp(1, 4096),
      request_timeout: Duration::from_millis(settings.request_timeout_ms.clamp(1_000, 300_000)),
    })
    .expect("diffcalc client already initialized");
}

fn client() -> &'static DiffcalcClient { DIFFCALC.get().expect("diffcalc client not initialized") }

#[derive(Debug)]
struct DiffcalcClient {
  client: Client,
  url: String,
  api_key: String,
  max_batch_size: usize,
  request_timeout: Duration,
}

#[derive(Clone, Copy)]
pub enum Operation {
  Hiscores,
  SimulateSingle,
  SimulateBatch,
}

impl Operation {
  fn as_str(self) -> &'static str {
    match self {
      Self::Hiscores => "hiscores",
      Self::SimulateSingle => "simulate_single",
      Self::SimulateBatch => "simulate_batch",
    }
  }
}

#[derive(Clone, Serialize)]
pub struct CalculationRequest {
  pub request_id: Option<String>,
  pub beatmap_id: i32,
  pub mods: Vec<Mod>,
  pub is_classic: bool,
  pub score: Option<ScoreInput>,
}

#[derive(Clone, Serialize)]
pub struct ScoreInput {
  /// Accuracy as a percentage in [0, 100].
  pub accuracy: f64,
  pub max_combo: Option<u32>,
  pub legacy_total_score: Option<i64>,
  pub statistics: Option<ScoreStatisticsInput>,
}

#[derive(Clone, Serialize)]
pub struct ScoreStatisticsInput {
  pub great: Option<u32>,
  pub ok: Option<u32>,
  pub meh: Option<u32>,
  pub miss: u32,
  pub large_tick_hit: Option<u32>,
  pub large_tick_miss: Option<u32>,
  pub small_tick_hit: Option<u32>,
  pub small_tick_miss: Option<u32>,
  pub slider_tail_hit: Option<u32>,
  pub large_bonus: Option<u32>,
  pub small_bonus: Option<u32>,
}

#[derive(Serialize)]
struct CalculationBatchRequest {
  operation: &'static str,
  calculations: Vec<CalculationRequest>,
}

#[derive(Deserialize)]
struct CalculationBatchResponse {
  #[allow(dead_code)]
  algorithm: AlgorithmInfo,
  results: Vec<CalculationResult>,
}

#[derive(Deserialize)]
struct AlgorithmInfo {
  #[allow(dead_code)]
  osu_game_package_version: String,
  #[allow(dead_code)]
  difficulty_version: i32,
}

#[derive(Clone, Deserialize)]
pub struct CalculationResult {
  pub request_id: Option<String>,
  pub beatmap_id: i32,
  pub difficulty: Option<DifficultyResult>,
  pub performance: Option<PerformanceResult>,
  pub error: Option<CalculationError>,
}

#[derive(Clone, Deserialize)]
pub struct DifficultyResult {
  pub stars: f64,
  pub aim: f64,
  pub speed: f64,
  pub flashlight: f64,
  pub reading: f64,
  pub slider_factor: f64,
  pub speed_note_count: f64,
  pub approach_rate: f64,
  pub overall_difficulty: f64,
  pub circle_size: f64,
  pub drain_rate: f64,
  pub clock_rate: f64,
}

#[derive(Clone, Deserialize)]
pub struct PerformanceResult {
  pub pp: f64,
  pub aim: f64,
  pub speed: f64,
  pub accuracy: f64,
  pub flashlight: f64,
  pub reading: f64,
  pub effective_miss_count: f64,
  pub speed_deviation: Option<f64>,
}

#[derive(Clone, Deserialize)]
pub struct CalculationError {
  pub code: String,
  pub message: String,
}

pub async fn calculate_all(
  operation: Operation,
  calculations: Vec<CalculationRequest>,
) -> Result<Vec<CalculationResult>, APIError> {
  if calculations.is_empty() {
    return Ok(Vec::new());
  }

  let mut results = Vec::with_capacity(calculations.len());
  for chunk in calculations.chunks(client().max_batch_size) {
    results.extend(calculate_batch(operation, chunk.to_vec()).await?);
  }
  Ok(results)
}

async fn calculate_batch(
  operation: Operation,
  calculations: Vec<CalculationRequest>,
) -> Result<Vec<CalculationResult>, APIError> {
  let started = Instant::now();
  let response = client()
    .client
    .post(format!("{}/v1/calculate", client().url))
    .header("X-Diffcalc-Key", &client().api_key)
    .timeout(client().request_timeout)
    .json(&CalculationBatchRequest {
      operation: operation.as_str(),
      calculations,
    })
    .send()
    .await;
  http_server::diffcalc_response_time_seconds(operation.as_str())
    .observe(started.elapsed().as_nanos() as u64);

  let response = match response {
    Ok(response) => response,
    Err(err) => {
      http_server::diffcalc_requests_total(operation.as_str(), "transport_error").inc();
      error!("diffcalc request failed: {err}");
      return Err(APIError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "Difficulty calculation service is unavailable".to_owned(),
      });
    },
  };

  if !response.status().is_success() {
    http_server::diffcalc_requests_total(operation.as_str(), "http_error").inc();
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let body = body.chars().take(512).collect::<String>();
    error!("diffcalc returned {status}: {body}");
    return Err(APIError {
      status: StatusCode::BAD_GATEWAY,
      message: "Difficulty calculation service rejected the request".to_owned(),
    });
  }

  let response = response
    .json::<CalculationBatchResponse>()
    .await
    .map_err(|err| {
      http_server::diffcalc_requests_total(operation.as_str(), "invalid_response").inc();
      error!("failed to decode diffcalc response: {err}");
      APIError {
        status: StatusCode::BAD_GATEWAY,
        message: "Difficulty calculation service returned an invalid response".to_owned(),
      }
    })?;
  http_server::diffcalc_requests_total(operation.as_str(), "success").inc();
  Ok(response.results)
}
