use axum::{extract::Json, http::StatusCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
  metrics::http_server,
  server::{APIError, SETTINGS},
};

#[derive(Deserialize, serde::Serialize)]
pub struct AnalyticsEvent {
  pub category: String,
  pub subcategory: String,
}

#[derive(Deserialize)]
pub struct AnalyticsRequest {
  pub event: AnalyticsEvent,
  pub verification: String,
}

#[derive(Deserialize)]
pub struct BatchAnalyticsRequest {
  pub events: Vec<AnalyticsEvent>,
  pub verification: String,
}

fn verify_payload(
  payload: &[AnalyticsEvent],
  salt: &str,
  verification: &str,
) -> Result<(), APIError> {
  let mut hasher = Sha256::new();
  for evt in payload {
    hasher.update(evt.category.as_bytes());
    hasher.update(evt.subcategory.as_bytes());
  }
  hasher.update(salt.as_bytes());
  let result = hasher.finalize();
  let hash = format!("{:x}", result);

  if hash != verification {
    warn!("Invalid analytics verification hash: expected {hash}, got {verification}");
    return Err(APIError {
      status: StatusCode::FORBIDDEN,
      message: "Invalid verification hash".to_owned(),
    });
  }

  Ok(())
}

pub async fn submit_event(Json(request): Json<AnalyticsRequest>) -> Result<(), APIError> {
  let settings = SETTINGS.get().unwrap();
  verify_payload(
    unsafe { std::slice::from_raw_parts(&request.event, 1) },
    &settings.analytics_salt,
    &request.verification,
  )?;

  http_server::analytics_events_total(request.event.category, request.event.subcategory).inc();

  Ok(())
}

pub async fn submit_batch_events(
  Json(request): Json<BatchAnalyticsRequest>,
) -> Result<(), APIError> {
  let settings = SETTINGS.get().unwrap();
  verify_payload(
    &request.events,
    &settings.analytics_salt,
    &request.verification,
  )?;

  for event in request.events {
    http_server::analytics_events_total(event.category, event.subcategory).inc();
  }

  Ok(())
}
