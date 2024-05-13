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

#[cfg(feature = "simulate_play")]
mod simulate_play {
  use axum::extract;
  use serde::Serialize;
  use std::{io::Read, str::FromStr};

  use rosu_pp::{Beatmap, Performance};
  use rosu_v2::model::GameMods;
  use tokio::sync::OnceCell;

  use super::*;

  static DB_POOL: OnceCell<sqlx::MySqlPool> = OnceCell::const_new();

  pub(super) async fn init_db_pool(db_url: &str) -> BootstrapResult<()> {
    let pool = sqlx::MySqlPool::connect(db_url).await?;
    DB_POOL.set(pool).unwrap();
    Ok(())
  }

  fn db_pool() -> &'static sqlx::MySqlPool { DB_POOL.get().expect("DB pool not initialized") }

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

  async fn simulate_play(
    beatmap: &Beatmap,
    params: &SimulatePlayQueryParams,
  ) -> Result<f64, APIError> {
    let mods =
      GameMods::from_str(params.mods.as_deref().unwrap_or_default()).map_err(|err| APIError {
        status: StatusCode::BAD_REQUEST,
        message: format!("Invalid mods: {err}"),
      })?;

    let diff_attrs = rosu_pp::Difficulty::new()
    .mods(mods.bits()) // HDHR
    .calculate(&beatmap);

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
    let pp = simulate_play(&beatmap, &params).await?;
    Ok(Json(SimulatePlayResponse { pp }))
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
    let mut pps = Vec::new();
    for params in params {
      let pp = simulate_play(&beatmap, &params).await?;
      pps.push(pp);
    }
    Ok(Json(BatchSimulatePlayResponse { pp: pps }))
  }
}

pub async fn start_server(settings: &ServerSettings) -> BootstrapResult<()> {
  set_client_info(settings.osu_client_id, settings.osu_client_secret.clone());

  #[cfg(feature = "simulate_play")]
  simulate_play::init_db_pool(&settings.sql.db_url).await?;

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
