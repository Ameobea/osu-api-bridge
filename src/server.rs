use std::{
  future::Future,
  marker::PhantomData,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};

use arc_swap::ArcSwap;
use axum::{
  extract::{Path, Query, Request},
  handler::Handler,
  http::StatusCode,
  response::{IntoResponse, Response},
  Json, Router,
};
use float_ord::FloatOrd;
use foundations::BootstrapResult;
use serde::Deserialize;
use tokio::sync::OnceCell;
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

#[cfg(feature = "sql")]
mod admin;
#[cfg(feature = "daily_challenge")]
mod daily_challenge;
#[cfg(feature = "simulate_play")]
mod simulate_play;

async fn index() -> &'static str { "osu-api-bridge up and running successfully!" }

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
  let hiscores_v2 = fetch_user_hiscores(user_id, params.mode, params.limit, params.offset).await?;

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

#[derive(Deserialize)]
struct ModeQueryParam {
  mode: Ruleset,
}

async fn get_user_scores_for_beatmap(
  Path((user_id, beatmap_id)): Path<(u64, u64)>,
  Query(params): Query<ModeQueryParam>,
) -> Result<Json<Vec<UserScoreOnBeatmap>>, APIError> {
  crate::osu_api::fetch_all_user_scores_for_beatmap(user_id, beatmap_id, params.mode)
    .await
    .map(Json)
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
  let mut scores =
    crate::osu_api::fetch_all_user_scores_for_beatmap(user_id, beatmap_id, mode).await?;

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
  // first check local DB to avoid osu! API roundtrip
  #[cfg(feature = "sql")]
  {
    let query = sqlx::query_scalar!("SELECT osu_id FROM users WHERE username = ?", username);
    match query.fetch_optional(crate::db::db_pool()).await {
      Ok(Some(user_id)) => {
        return Ok(Json(user_id as u64));
      },
      Err(err) => {
        error!("Error fetching user_id from DB: {err}");
      },
      _ => (),
    }
  }

  crate::osu_api::fetch_user_id(&username, params.mode)
    .await
    .map(Json)
}

async fn get_username(Path(user_id): Path<u64>) -> Result<Json<String>, APIError> {
  // first check local DB to avoid osu! API roundtrip
  #[cfg(feature = "sql")]
  {
    let query = sqlx::query_scalar!("SELECT username FROM users WHERE osu_id = ?", user_id);
    match query.fetch_optional(crate::db::db_pool()).await {
      Ok(Some(username)) => {
        return Ok(Json(username));
      },
      Err(err) => {
        error!("Error fetching username from DB: {}", err);
      },
      _ => (),
    }
  }

  let username_opt = crate::osu_api::fetch_username(user_id).await?;
  match username_opt {
    Some(username) => Ok(Json(username)),
    None => Err(APIError {
      status: StatusCode::NOT_FOUND,
      message: format!("User with id {} not found", user_id),
    }),
  }
}

#[derive(Clone)]
struct InstrumentedHandler<H, S> {
  pub endpoint_name: &'static str,
  pub handler: H,
  pub state: PhantomData<S>,
}

#[pin_project::pin_project]
struct InstrumentedHandlerFuture<F> {
  #[pin]
  inner: F,
  endpoint_name: &'static str,
}

impl<F: Unpin + Future<Output = Response>> Future for InstrumentedHandlerFuture<F> {
  type Output = Response;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    http_server::requests_total(self.endpoint_name).inc();

    let endpoint_name = self.endpoint_name;
    let this = self.project();
    let inner = this.inner;
    let inner_poll = inner.poll(cx);

    match inner_poll {
      Poll::Ready(res) => {
        if res.status().is_success() {
          http_server::requests_success_total(endpoint_name).inc();
        } else {
          http_server::requests_failed_total(endpoint_name).inc();
        }
        Poll::Ready(res)
      },
      Poll::Pending => Poll::Pending,
    }
  }
}

impl<T, H: Handler<T, S>, S: Clone + Send + Sync + 'static> Handler<T, S>
  for InstrumentedHandler<H, S>
where
  H::Future: Unpin,
{
  type Future = InstrumentedHandlerFuture<H::Future>;

  fn call(self, req: Request, state: S) -> Self::Future {
    let res_future = self.handler.call(req, state);
    InstrumentedHandlerFuture {
      inner: res_future,
      endpoint_name: self.endpoint_name,
    }
  }
}

fn instrument_handler<T: 'static, H: Handler<T, S> + 'static, S: Clone + Send + 'static>(
  endpoint_name: &'static str,
  handler: H,
) -> InstrumentedHandler<H, S> {
  InstrumentedHandler {
    endpoint_name,
    handler,
    state: PhantomData,
  }
}

static SETTINGS: OnceCell<ServerSettings> = OnceCell::const_new();

pub async fn start_server(settings: &ServerSettings) -> BootstrapResult<()> {
  set_client_info(settings.osu_client_id, settings.osu_client_secret.clone());

  SETTINGS
    .set(settings.clone())
    .expect("SETTINGS already set?");

  #[cfg(feature = "sql")]
  crate::db::init_db_pool(&settings.sql.db_url).await?;

  let mut router = Router::new()
    .route("/", axum::routing::get(instrument_handler("index", index)))
    .route(
      "/users/{user_id}/hiscores",
      axum::routing::get(instrument_handler("get_hiscores", get_hiscores)),
    )
    .route(
      "/users/{user_id}/beatmaps/{beatmap_id}/scores",
      axum::routing::get(instrument_handler(
        "get_user_scores_for_beatmap",
        get_user_scores_for_beatmap,
      )),
    )
    .route(
      "/users/{user_id}/beatmaps/{beatmap_id}/best",
      axum::routing::get(instrument_handler(
        "get_user_best_score_for_beatmap",
        get_user_best_score_for_beatmap,
      )),
    )
    .route(
      "/users/{username}/id",
      axum::routing::get(instrument_handler("get_user_id", get_user_id)),
    )
    .route(
      "/users/{user_id}/username",
      axum::routing::get(instrument_handler("get_username", get_username)),
    );

  #[cfg(feature = "simulate_play")]
  {
    router = router
      .route(
        "/beatmaps/{beatmap_id}/simulate",
        axum::routing::get(instrument_handler(
          "simulate_play",
          simulate_play::simulate_play_route,
        )),
      )
      .route(
        "/beatmaps/{beatmap_id}/simulate/batch",
        axum::routing::post(instrument_handler(
          "batch_simulate_play",
          simulate_play::batch_simulate_play_route,
        )),
      );
  }

  #[cfg(feature = "daily_challenge")]
  {
    router = router
      .route(
        "/daily-challenge/backfill",
        axum::routing::post(instrument_handler(
          "backfill_daily_challenge",
          daily_challenge::backfill_daily_challenges,
        )),
      )
      .route(
        "/daily-challenge/recompute-user-ranks",
        axum::routing::post(instrument_handler(
          "recompute_user_ranks",
          daily_challenge::recompute_all_user_ranks,
        )),
      )
      .route(
        "/daily-challenge/user/{user_id}/history",
        axum::routing::get(instrument_handler(
          "get_user_daily_challenge_history",
          daily_challenge::get_user_daily_challenge_history,
        )),
      )
      .route(
        "/daily-challenge/user/{user_id}/day/{day_id}",
        axum::routing::get(instrument_handler(
          "get_user_daily_challenge_for_day",
          daily_challenge::get_user_daily_challenge_for_day,
        )),
      )
      .route(
        "/daily-challenge/user/{user_id}/stats",
        axum::routing::get(instrument_handler(
          "get_user_daily_challenge_stats",
          daily_challenge::get_user_daily_challenge_stats,
        )),
      )
      .route(
        "/daily-challenge/day/{day_id}/stats",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_stats_for_day",
          daily_challenge::get_daily_challenge_stats_for_day,
        )),
      )
      .route(
        "/daily-challenge/day/{day_id}/rankings",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_for_day",
          daily_challenge::get_daily_challenge_rankings_for_day,
        )),
      )
      .route(
        "/daily-challenge/rankings",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings",
          daily_challenge::get_daily_challenge_total_score_rankings,
        )),
      )
      .route(
        "/daily-challenge/rankings/percent/50",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_percent_50",
          daily_challenge::get_daily_challenge_top_50_percent_rankings,
        )),
      )
      .route(
        "/daily-challenge/rankings/percent/10",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_percent_10",
          daily_challenge::get_daily_challenge_top_10_percent_rankings,
        )),
      )
      .route(
        "/daily-challenge/rankings/percent/1",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_percent_1",
          daily_challenge::get_daily_challenge_top_1_percent_rankings,
        )),
      )
      .route(
        "/daily-challenge/global-stats",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_global_stats",
          daily_challenge::get_daily_challenge_global_stats,
        )),
      )
      .route(
        "/daily-challenge/latest-day-id",
        axum::routing::get(instrument_handler(
          "get_latest_daily_challenge_day_id",
          daily_challenge::get_latest_daily_challenge_day_id,
        )),
      )
  }

  #[cfg(feature = "sql")]
  {
    router = router.route(
      "/verify-best-plays",
      axum::routing::post(instrument_handler(
        "verify_best_plays",
        admin::verify_best_plays,
      )),
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
