use std::{convert::Infallible, sync::LazyLock};

use axum::{
  extract::Json,
  http::{HeaderMap, StatusCode},
  response::{
    sse::{Event, KeepAlive, Sse},
    IntoResponse,
  },
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

use crate::{
  metrics::http_server,
  server::{APIError, SETTINGS},
};

#[derive(Deserialize, serde::Serialize)]
pub struct AnalyticsEvent {
  pub category: String,
  pub subcategory: String,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub payload: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct AnalyticsRequest {
  pub event: AnalyticsEvent,
  pub verification: String,
  #[serde(default)]
  pub project: Option<String>,
  #[serde(default)]
  pub session_id: Option<String>,
}

#[derive(Deserialize)]
pub struct BatchAnalyticsRequest {
  pub events: Vec<AnalyticsEvent>,
  pub verification: String,
  #[serde(default)]
  pub project: Option<String>,
  #[serde(default)]
  pub session_id: Option<String>,
}

// Hash covers category+subcategory+salt only; `project`/`payload`/`session_id` are deliberately
// excluded so pre-existing producers that predate those fields keep verifying.
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
  let hash = hex::encode(result);

  if hash != verification {
    warn!("Invalid analytics verification hash: expected {hash}, got {verification}");
    return Err(APIError {
      status: StatusCode::FORBIDDEN,
      message: "Invalid verification hash".to_owned(),
    });
  }

  Ok(())
}

fn resolve_project(explicit: Option<String>, headers: &HeaderMap) -> String {
  explicit
    .filter(|p| !p.is_empty())
    .or_else(|| {
      headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .map(|origin| {
          origin
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_owned()
        })
        .filter(|p| !p.is_empty())
    })
    .unwrap_or_else(|| "unknown".to_owned())
}

struct EventMetadata {
  project: String,
  session_id: Option<String>,
  ip_hash: Option<String>,
  user_agent: Option<String>,
}

fn extract_metadata(
  explicit_project: Option<String>,
  session_id: Option<String>,
  headers: &HeaderMap,
  salt: &str,
) -> EventMetadata {
  let ip_hash = headers
    .get("x-forwarded-for")
    .or_else(|| headers.get("x-real-ip"))
    .and_then(|v| v.to_str().ok())
    .and_then(|v| v.split(',').next())
    .map(|ip| {
      let mut hasher = Sha256::new();
      hasher.update(ip.trim().as_bytes());
      hasher.update(salt.as_bytes());
      hex::encode(hasher.finalize())[..16].to_owned()
    });
  let user_agent = headers
    .get("user-agent")
    .and_then(|v| v.to_str().ok())
    .map(|ua| ua.chars().take(255).collect());

  EventMetadata {
    project: resolve_project(explicit_project, headers),
    session_id: session_id.filter(|s| !s.is_empty()).map(|mut s| {
      s.truncate(32);
      s
    }),
    ip_hash,
    user_agent,
  }
}

#[cfg(feature = "sql")]
async fn insert_events(meta: &EventMetadata, events: &[AnalyticsEvent]) {
  let mut conn = match crate::db::conn().await {
    Ok(conn) => conn,
    Err(err) => {
      error!(
        "Error acquiring DB connection for analytics insert: {}",
        err.message
      );
      return;
    },
  };

  for evt in events {
    let res = sqlx::query(
      "INSERT INTO analytics_events (project, category, subcategory, payload, session_id, \
       ip_hash, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&meta.project)
    .bind(&evt.category)
    .bind(&evt.subcategory)
    .bind(evt.payload.as_ref().map(|p| p.to_string()))
    .bind(&meta.session_id)
    .bind(&meta.ip_hash)
    .bind(&meta.user_agent)
    .execute(&mut *conn)
    .await;
    if let Err(err) = res {
      error!("Error inserting analytics event: {err}");
    }
  }
}

#[cfg(not(feature = "sql"))]
async fn insert_events(_meta: &EventMetadata, _events: &[AnalyticsEvent]) {}

fn record_events(meta: &EventMetadata, events: &[AnalyticsEvent]) {
  for evt in events {
    http_server::analytics_events_total(
      meta.project.clone(),
      evt.category.clone(),
      evt.subcategory.clone(),
    )
    .inc();
  }
}

static LIVE_EVENTS: LazyLock<tokio::sync::broadcast::Sender<String>> =
  LazyLock::new(|| tokio::sync::broadcast::channel(256).0);

// Payloads are broadcast for the live viz; session IDs / UAs / ip hashes stay off the public stream
fn publish_events(meta: &EventMetadata, events: &[AnalyticsEvent]) {
  if LIVE_EVENTS.receiver_count() == 0 {
    return;
  }
  for evt in events {
    let msg = serde_json::json!({
      "project": meta.project,
      "category": evt.category,
      "subcategory": evt.subcategory,
      "payload": evt.payload,
      "ts": chrono::Utc::now().timestamp_millis(),
    });
    let _ = LIVE_EVENTS.send(msg.to_string());
  }
}

pub async fn stream_events() -> impl IntoResponse {
  let stream = BroadcastStream::new(LIVE_EVENTS.subscribe())
    .filter_map(|msg| msg.ok())
    .map(|msg| Ok::<_, Infallible>(Event::default().data(msg)));
  (
    [("x-accel-buffering", "no")],
    Sse::new(stream).keep_alive(KeepAlive::default()),
  )
}

pub async fn submit_event(
  headers: HeaderMap,
  Json(request): Json<AnalyticsRequest>,
) -> Result<(), APIError> {
  let settings = SETTINGS.get().unwrap();
  verify_payload(
    std::slice::from_ref(&request.event),
    &settings.analytics_salt,
    &request.verification,
  )?;

  let meta = extract_metadata(
    request.project,
    request.session_id,
    &headers,
    &settings.analytics_salt,
  );
  let events = [request.event];
  record_events(&meta, &events);
  publish_events(&meta, &events);
  insert_events(&meta, &events).await;

  Ok(())
}

pub async fn submit_batch_events(
  headers: HeaderMap,
  Json(request): Json<BatchAnalyticsRequest>,
) -> Result<(), APIError> {
  let settings = SETTINGS.get().unwrap();
  verify_payload(
    &request.events,
    &settings.analytics_salt,
    &request.verification,
  )?;

  let meta = extract_metadata(
    request.project,
    request.session_id,
    &headers,
    &settings.analytics_salt,
  );
  record_events(&meta, &request.events);
  publish_events(&meta, &request.events);
  insert_events(&meta, &request.events).await;

  Ok(())
}
