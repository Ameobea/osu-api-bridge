use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{
  extract::{Path, Query},
  http::StatusCode,
  response::{IntoResponse, Response},
  Json, Router,
};
use float_ord::FloatOrd;
use foundations::BootstrapResult;
use serde::Deserialize;
use tower_http::{
  cors,
  trace::{DefaultMakeSpan, DefaultOnResponse},
};
use tracing::Level;

use crate::{
  metrics::http_server,
  oauth::set_client_info,
  osu_api::{fetch_user_hiscores, HiscoreV1, Ruleset, UserScoreOnBeatmap},
  settings::ServerSettings,
};

#[cfg(feature = "daily_challenge")]
mod daily_challenge;
#[cfg(feature = "simulate_play")]
mod simulate_play;

async fn index() -> &'static str {
  http_server::requests_total("index").inc();
  http_server::requests_success_total("index").inc();
  "osu-api-bridge up and running successfully!"
}

#[derive(Deserialize)]
struct GetHiscoresParams {
  mode: Ruleset,
  limit: Option<u8>,
  offset: Option<u8>,
}

#[derive(Debug)]
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
  http_server::requests_total("get_hiscores").inc();

  let res = async move {
    let hiscores_v2 =
      fetch_user_hiscores(user_id, params.mode, params.limit, params.offset).await?;

    let hiscores_v1 = hiscores_v2
      .into_iter()
      .map(|hs| hs.into_v1())
      .collect::<Result<Vec<_>, _>>()
      .map_err(|err| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Error converting hiscores to v1 format: {}", err),
      })?;

    Ok(Json(hiscores_v1))
  };
  match res.await {
    Ok(res) => {
      http_server::requests_success_total("get_hiscores").inc();
      Ok(res)
    },
    Err(err) => {
      http_server::requests_failed_total("get_hiscores").inc();
      Err(err)
    },
  }
}

#[derive(Deserialize)]
struct ModeQueryParam {
  mode: Ruleset,
}

async fn get_user_scores_for_beatmap(
  Path((user_id, beatmap_id)): Path<(u64, u64)>,
  Query(params): Query<ModeQueryParam>,
) -> Result<Json<Vec<UserScoreOnBeatmap>>, APIError> {
  let endpoint_name = "get_user_scores_for_beatmap";
  http_server::requests_total(endpoint_name).inc();

  match crate::osu_api::fetch_all_user_scores_for_beatmap(user_id, beatmap_id, params.mode).await {
    Ok(hiscores) => {
      http_server::requests_success_total(endpoint_name).inc();
      Ok(Json(hiscores))
    },
    Err(err) => {
      http_server::requests_failed_total(endpoint_name).inc();
      Err(err)
    },
  }
}

#[derive(Deserialize)]
struct GetBestScoreParams {
  mode: Ruleset,
  // Like "HDDT", "FL", "", etc.
  mods: Option<String>,
}

async fn get_user_best_score_for_beatmap(
  Path((user_id, beatmap_id)): Path<(u64, u64)>,
  Query(GetBestScoreParams { mode, mods }): Query<GetBestScoreParams>,
) -> Result<Json<Option<UserScoreOnBeatmap>>, APIError> {
  let endpoint_name = "get_user_best_score_for_beatmap";
  http_server::requests_total(endpoint_name).inc();

  let mut scores =
    match crate::osu_api::fetch_all_user_scores_for_beatmap(user_id, beatmap_id, mode).await {
      Ok(hiscores) => {
        http_server::requests_success_total(endpoint_name).inc();
        hiscores
      },
      Err(err) => {
        http_server::requests_failed_total(endpoint_name).inc();
        return Err(err);
      },
    };

  // split every two characters
  let required_mods: Vec<String> = match mods {
    Some(mods) => mods
      .to_uppercase()
      .as_str()
      .chars()
      .collect::<Vec<char>>()
      .chunks(2)
      .map(|c| c.iter().collect())
      .collect(),
    None => vec![],
  };

  fn compare_mod(mod_a: &str, mod_b: &str) -> bool {
    if mod_a == mod_b {
      return true;
    }
    let mod_a_is_dt = mod_a == "DT" || mod_a == "NC";
    let mod_b_is_dt = mod_b == "DT" || mod_b == "NC";
    if mod_a_is_dt && mod_b_is_dt {
      return true;
    }

    false
  }

  scores.retain(|score| {
    score.passed
      && required_mods.iter().all(|required_mod| {
        score
          .mods
          .iter()
          .any(|score_mod| compare_mod(required_mod, score_mod))
      })
  });
  let top_score = scores
    .into_iter()
    .max_by_key(|score| FloatOrd(score.pp.unwrap_or(-1.)));
  Ok(Json(top_score))
}

async fn get_user_id(
  Path(username): Path<String>,
  Query(params): Query<ModeQueryParam>,
) -> Result<Json<u64>, APIError> {
  let endpoint_name = "get_user_id";
  http_server::requests_total(endpoint_name).inc();
  match crate::osu_api::fetch_user_id(&username, params.mode).await {
    Ok(user_id) => {
      http_server::requests_success_total(endpoint_name).inc();
      Ok(Json(user_id))
    },
    Err(err) => {
      http_server::requests_failed_total(endpoint_name).inc();
      Err(err)
    },
  }
}

lazy_static::lazy_static! {
  static ref SETTINGS: ArcSwap<ServerSettings> = ArcSwap::new(Arc::new(ServerSettings::default()));
}

pub async fn start_server(settings: &ServerSettings) -> BootstrapResult<()> {
  set_client_info(settings.osu_client_id, settings.osu_client_secret.clone());

  SETTINGS.store(Arc::new(settings.clone()));

  #[cfg(feature = "sql")]
  crate::db::init_db_pool(&settings.sql.db_url).await?;

  let mut router = Router::new()
    .route("/", axum::routing::get(index))
    .route("/users/:user_id/hiscores", axum::routing::get(get_hiscores))
    .route(
      "/users/:user_id/beatmaps/:beatmap_id/scores",
      axum::routing::get(get_user_scores_for_beatmap),
    )
    .route(
      "/users/:user_id/beatmaps/:beatmap_id/best",
      axum::routing::get(get_user_best_score_for_beatmap),
    )
    .route("/users/:username/id", axum::routing::get(get_user_id));

  #[cfg(feature = "simulate_play")]
  {
    router = router
      .route(
        "/beatmaps/:beatmap_id/simulate",
        axum::routing::get(simulate_play::simulate_play_route),
      )
      .route(
        "/beatmaps/:beatmap_id/simulate/batch",
        axum::routing::post(simulate_play::batch_simulate_play_route),
      );
  }

  #[cfg(feature = "daily_challenge")]
  {
    router = router
      .route(
        "/daily-challenge/backfill",
        axum::routing::post(daily_challenge::backfill_daily_challenges),
      )
      .route(
        "/daily-challenge/:user_id/history",
        axum::routing::get(daily_challenge::get_user_daily_challenge_history),
      );
  }

  router = router
    .layer(
      tower_http::cors::CorsLayer::new()
        .allow_origin(cors::Any)
        .allow_headers(cors::Any)
        .allow_methods(cors::Any),
    )
    .layer(
      tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(DefaultOnResponse::default().level(Level::INFO)),
    );
  let addr = format!("0.0.0.0:{}", settings.port);
  info!("Server is listening on http://{}", addr);
  let listener = tokio::net::TcpListener::bind(addr).await?;
  axum::serve(listener, router).await?;
  Ok(())
}
