#![allow(unused_braces)]

use std::sync::Arc;

use axum::{
  extract::{Path, Query, Request},
  http::StatusCode,
  middleware::{self, Next},
  response::{IntoResponse, Response},
  Json, Router,
};
use foundations::BootstrapResult;
use serde::Deserialize;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse};
use tracing::Level;

use crate::{
  oauth::set_client_info,
  osu_api::{fetch_user_hiscores, HiscoreV1, Ruleset},
  settings::ServerSettings,
};

async fn metrics_middleware(req: Request, next: Next) -> Response {
  let path = Arc::new(req.uri().path().to_owned());
  crate::metrics::http_server::requests_total(&path).inc();
  let res = next.run(req).await;
  if res.status().is_success() {
    crate::metrics::http_server::requests_success_total(&path).inc();
  } else {
    crate::metrics::http_server::requests_failed_total(&path, res.status().as_u16()).inc();
  }
  res
}

async fn index() -> &'static str { "osu-api-bridge up and running successfully!" }

#[derive(Deserialize)]
struct GetHiscoresParams {
  mode: Ruleset,
  limit: Option<u8>,
}

pub struct APIError {
  pub status: StatusCode,
  pub message: String,
}

impl IntoResponse for APIError {
  fn into_response(self) -> Response { (self.status, self.message).into_response() }
}

async fn get_hiscores(
  Path(user_id): Path<u64>,
  Query(params): Query<GetHiscoresParams>,
) -> Result<Json<Vec<HiscoreV1>>, APIError> {
  let hiscores_v2 = fetch_user_hiscores(user_id, params.mode, params.limit).await?;

  let hiscores_v1 = hiscores_v2
    .into_iter()
    .map(|hs| hs.into_v1())
    .collect::<Result<Vec<_>, _>>()
    .map_err(|err| APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error converting hiscores to v1 format: {}", err),
    })?;

  Ok(Json(hiscores_v1))
}

pub async fn start_server(settings: &ServerSettings) -> BootstrapResult<()> {
  set_client_info(settings.osu_client_id, settings.osu_client_secret.clone());

  let router = Router::new()
    .route("/", axum::routing::get(index))
    .route("/users/:user_id/hiscores", axum::routing::get(get_hiscores))
    .layer(
      tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(DefaultOnResponse::default().level(Level::INFO)),
    )
    .layer(middleware::from_fn(metrics_middleware));
  let addr = format!("0.0.0.0:{}", settings.port);
  info!("Server is listening on http://{}", addr);
  let listener = tokio::net::TcpListener::bind(addr).await?;
  axum::serve(listener, router).await?;
  Ok(())
}
