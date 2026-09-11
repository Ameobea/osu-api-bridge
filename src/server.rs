use std::{
  future::Future,
  marker::PhantomData,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
  time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use axum::{
  extract::{DefaultBodyLimit, Path, Query, Request},
  handler::Handler,
  http::StatusCode,
  response::{IntoResponse, Response},
  Json, Router,
};
use float_ord::FloatOrd;
use foundations::BootstrapResult;
use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tower_http::{
  cors,
  trace::{DefaultMakeSpan, DefaultOnResponse},
};
use tracing::Level;

use crate::{
  metrics::http_server,
  oauth::set_client_info,
  osu_api::{
    fetch_user_hiscores, BeatmapDifficulties, HiscoreV1, HiscoreV2, OsutrackDbBeatmap,
    OsutrackUserStats, Ruleset, UserScoreOnBeatmap,
  },
  settings::ServerSettings,
};

#[cfg(feature = "sql")]
mod admin;
mod analysis;
mod analytics;
#[cfg(feature = "daily_challenge")]
mod daily_challenge;
#[cfg(feature = "daily_challenge")]
mod embed;
#[cfg(feature = "sql")]
mod osutrack_update;
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

#[derive(Deserialize)]
struct GetUserStatsParams {
  mode: Ruleset,
}

async fn get_user_stats(
  Path(username): Path<String>,
  Query(params): Query<GetUserStatsParams>,
) -> Result<Json<OsutrackUserStats>, APIError> {
  let Some(mode) = Ruleset::from_mode_value(params.mode.mode_value()) else {
    return Err(APIError {
      status: StatusCode::BAD_REQUEST,
      message: format!("Invalid mode value: {}", params.mode.mode_value()),
    });
  };
  let stats = crate::osu_api::fetch_osutrack_user_stats(&username, mode).await?;
  Ok(Json(stats))
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
      message: format!("Error converting hiscores to v1 format: {err}"),
    })?;

  Ok(Json(hiscores_v1))
}

#[derive(Clone, Serialize)]
struct PerfAttrs {
  pub earned: OsuPerformanceAttributes,
  pub max: OsuPerformanceAttributes,
}

#[derive(Serialize)]
struct GetHiscoresV2Response {
  pub hiscores: Vec<HiscoreV2>,
  pub beatmaps: FxHashMap<u64, OsutrackDbBeatmap>,
  pub difficulties: FxHashMap<u64, BeatmapDifficulties>,
  pub performance_attrs: FxHashMap<i64, PerfAttrs>,
  pub attrs_with_mods: FxHashMap<u64, BeatmapAttrs>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OsuPerformanceAttributes {
  /// The final performance points.
  pub pp: f64,
  /// The accuracy portion of the final pp.
  pub pp_acc: f64,
  /// The aim portion of the final pp.
  pub pp_aim: f64,
  /// The flashlight portion of the final pp.
  pub pp_flashlight: f64,
  /// The speed portion of the final pp.
  pub pp_speed: f64,
  /// The reading portion of the final pp.
  pub pp_reading: f64,
  /// Misses including an approximated amount of slider breaks
  pub effective_miss_count: f64,
  /// Approximated unstable-rate
  pub speed_deviation: Option<f64>,
}

impl From<&crate::diffcalc::PerformanceResult> for OsuPerformanceAttributes {
  fn from(attr: &crate::diffcalc::PerformanceResult) -> Self {
    OsuPerformanceAttributes {
      pp: attr.pp,
      pp_acc: attr.accuracy,
      pp_aim: attr.aim,
      pp_flashlight: attr.flashlight,
      pp_speed: attr.speed,
      pp_reading: attr.reading,
      effective_miss_count: attr.effective_miss_count,
      speed_deviation: attr.speed_deviation,
    }
  }
}

#[derive(Clone, Serialize)]
pub struct BeatmapAttrs {
  pub cs: f64,
  pub ar: f64,
  pub od: f64,
  pub hp: f64,
  pub clock_rate: f64,
}

async fn compute_beatmap_difficulties(
  hiscores: &[HiscoreV2],
) -> Result<
  (
    Vec<Option<BeatmapDifficulties>>,
    Vec<Option<BeatmapAttrs>>,
    Vec<Option<PerfAttrs>>,
  ),
  APIError,
> {
  let _timer = http_server::compute_beatmap_difficulties_duration().start_timer();
  fn count(value: Option<i64>, name: &str) -> Result<Option<u32>, APIError> {
    value
      .map(|value| {
        u32::try_from(value).map_err(|_| APIError {
          status: StatusCode::BAD_GATEWAY,
          message: format!("osu! returned an invalid {name} count"),
        })
      })
      .transpose()
  }

  let mut requests = Vec::with_capacity(hiscores.len() * 2);
  for (index, hiscore) in hiscores.iter().enumerate() {
    let beatmap_id = i32::try_from(hiscore.beatmap_id).map_err(|_| APIError {
      status: StatusCode::BAD_GATEWAY,
      message: "osu! returned an invalid beatmap ID".to_owned(),
    })?;
    let is_classic = hiscore.mods.iter().any(|m| m.acronym == "CL");
    let has_main_statistics = hiscore.statistics.great.is_some()
      || hiscore.statistics.ok.is_some()
      || hiscore.statistics.meh.is_some();
    let statistics = crate::diffcalc::ScoreStatisticsInput {
      great: if has_main_statistics {
        Some(count(hiscore.statistics.great, "great")?.unwrap_or(0))
      } else {
        None
      },
      ok: if has_main_statistics {
        Some(count(hiscore.statistics.ok, "ok")?.unwrap_or(0))
      } else {
        None
      },
      meh: if has_main_statistics {
        Some(count(hiscore.statistics.meh, "meh")?.unwrap_or(0))
      } else {
        None
      },
      miss: count(hiscore.statistics.miss, "miss")?.unwrap_or(0),
      large_tick_hit: count(hiscore.statistics.large_tick_hit, "large_tick_hit")?,
      large_tick_miss: count(hiscore.statistics.large_tick_miss, "large_tick_miss")?,
      small_tick_hit: count(hiscore.statistics.small_tick_hit, "small_tick_hit")?,
      small_tick_miss: count(hiscore.statistics.small_tick_miss, "small_tick_miss")?,
      slider_tail_hit: count(hiscore.statistics.slider_tail_hit, "slider_tail_hit")?,
      large_bonus: count(hiscore.statistics.large_bonus, "large_bonus")?,
      small_bonus: count(hiscore.statistics.small_bonus, "small_bonus")?,
    };
    let max_combo = u32::try_from(hiscore.max_combo).map_err(|_| APIError {
      status: StatusCode::BAD_GATEWAY,
      message: "osu! returned an invalid max combo".to_owned(),
    })?;

    requests.push(crate::diffcalc::CalculationRequest {
      request_id: Some(format!("{index}:earned")),
      beatmap_id,
      mods: hiscore.mods.clone(),
      is_classic,
      score: Some(crate::diffcalc::ScoreInput {
        accuracy: hiscore.accuracy * 100.0,
        max_combo: Some(max_combo),
        legacy_total_score: hiscore.legacy_total_score,
        statistics: Some(statistics),
      }),
    });
    requests.push(crate::diffcalc::CalculationRequest {
      request_id: Some(format!("{index}:max")),
      beatmap_id,
      mods: hiscore.mods.clone(),
      is_classic,
      score: Some(crate::diffcalc::ScoreInput {
        accuracy: 100.0,
        max_combo: None,
        legacy_total_score: None,
        statistics: None,
      }),
    });
  }

  let results =
    crate::diffcalc::calculate_all(crate::diffcalc::Operation::Hiscores, requests).await?;
  if results.len() != hiscores.len() * 2 {
    return Err(APIError {
      status: StatusCode::BAD_GATEWAY,
      message: "Difficulty calculation service returned the wrong result count".to_owned(),
    });
  }

  let mut diffs = Vec::with_capacity(hiscores.len());
  let mut attrs = Vec::with_capacity(hiscores.len());
  let mut perfs = Vec::with_capacity(hiscores.len());
  for (hiscore, pair) in hiscores.iter().zip(results.chunks_exact(2)) {
    let earned = &pair[0];
    let max = &pair[1];
    if let Some(error) = earned.error.as_ref().or(max.error.as_ref()) {
      warn!(
        "diffcalc failed for beatmap {}: {}: {}",
        hiscore.beatmap_id, error.code, error.message
      );
    }

    let difficulty = earned.difficulty.as_ref().or(max.difficulty.as_ref());
    diffs.push(difficulty.map(|diff| BeatmapDifficulties {
      score_id: hiscore.build_score_id(),
      difficulty_aim: diff.aim,
      difficulty_speed: diff.speed,
      difficulty_flashlight: diff.flashlight,
      difficulty_reading: diff.reading,
      speed_note_count: diff.speed_note_count,
      slider_factor: diff.slider_factor,
      stars: diff.stars,
    }));
    attrs.push(difficulty.map(|diff| BeatmapAttrs {
      cs: diff.circle_size,
      ar: diff.approach_rate,
      od: diff.overall_difficulty,
      hp: diff.drain_rate,
      clock_rate: diff.clock_rate,
    }));

    let perf = earned
      .performance
      .as_ref()
      .zip(max.performance.as_ref())
      .map(|(earned, max)| PerfAttrs {
        earned: OsuPerformanceAttributes::from(earned),
        max: OsuPerformanceAttributes::from(max),
      });
    perfs.push(perf);
  }

  Ok((diffs, attrs, perfs))
}

async fn get_hiscores_v2(
  Path(user_id): Path<u64>,
  Query(params): Query<GetHiscoresParams>,
) -> Result<Json<GetHiscoresV2Response>, APIError> {
  let hiscores_v2 = fetch_user_hiscores(user_id, params.mode, params.limit, params.offset).await?;

  let beatmap_ids = hiscores_v2
    .iter()
    .map(|hs| hs.beatmap_id)
    .collect::<Vec<i64>>();
  let beatmaps_meta = if beatmap_ids.is_empty() {
    FxHashMap::default()
  } else {
    let beatmap_ids_string = beatmap_ids
      .iter()
      .map(|id| id.to_string())
      .collect::<Vec<_>>()
      .join(",");
    let query = format!(
      "SELECT beatmapset_id,beatmap_id,approved,approved_date,last_update,total_length,hit_length,\
       version,artist,title,creator,bpm,source,difficultyrating,diff_size,diff_overall,\
       diff_approach,diff_drain,mode FROM beatmaps WHERE beatmap_id IN ({beatmap_ids_string})"
    );
    sqlx::query_as::<_, OsutrackDbBeatmap>(sqlx::AssertSqlSafe(query))
      .fetch_all(crate::db::db_pool())
      .await
      .map_err(|err| APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("Error fetching beatmaps from DB: {err}"),
      })?
      .into_iter()
      .map(|bm| (bm.beatmap_id as u64, bm))
      .collect()
  };

  let computed_diffs = if params.mode == Ruleset::Osu {
    Some(compute_beatmap_difficulties(&hiscores_v2).await?)
  } else {
    None
  };

  let mut difficulties_by_beatmap_id: FxHashMap<u64, BeatmapDifficulties> = FxHashMap::default();
  let mut performance_attrs_by_score_id: FxHashMap<i64, PerfAttrs> = FxHashMap::default();

  // TODO: would have to handle mode-specific difficulties later
  if let Some((difficulties, _attrs_with_mods, performance_attrs)) = computed_diffs.as_ref() {
    for (i, hiscore) in hiscores_v2.iter().enumerate() {
      if let Some(diff) = &difficulties[i] {
        difficulties_by_beatmap_id.insert(hiscore.beatmap_id as u64, diff.clone());
      }
      if let Some(perf) = &performance_attrs[i] {
        performance_attrs_by_score_id.insert(hiscore.id, perf.clone());
      }
    }
  }

  let attrs_with_mods: FxHashMap<u64, BeatmapAttrs> =
    if let Some((_difficulties, attrs_with_mods, _performance_attrs)) = computed_diffs.as_ref() {
      hiscores_v2
        .iter()
        .enumerate()
        .filter_map(|(i, hiscore)| {
          attrs_with_mods[i]
            .as_ref()
            .map(|attrs| (hiscore.beatmap_id as u64, attrs.clone()))
        })
        .collect()
    } else {
      Default::default()
    };

  Ok(Json(GetHiscoresV2Response {
    hiscores: hiscores_v2,
    beatmaps: beatmaps_meta,
    difficulties: difficulties_by_beatmap_id,
    attrs_with_mods,
    performance_attrs: performance_attrs_by_score_id,
  }))
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

    let mod_a_is_ht = mod_a == "HT" || mod_a == "DC";
    let mod_b_is_ht = mod_b == "HT" || mod_b == "DC";
    if mod_a_is_ht && mod_b_is_ht {
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
  started: Instant,
}

impl<F: Unpin + Future<Output = Response>> Future for InstrumentedHandlerFuture<F> {
  type Output = Response;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let endpoint_name = self.endpoint_name;
    let this = self.project();
    let inner = this.inner;
    let inner_poll = inner.poll(cx);

    match inner_poll {
      Poll::Ready(res) => {
        http_server::request_duration_seconds(endpoint_name)
          .observe(this.started.elapsed().as_nanos() as u64);
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
    http_server::requests_total(self.endpoint_name).inc();
    let res_future = self.handler.call(req, state);
    InstrumentedHandlerFuture {
      inner: res_future,
      endpoint_name: self.endpoint_name,
      started: Instant::now(),
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

  #[cfg(feature = "simulate_play")]
  crate::diffcalc::init(&settings.diffcalc);

  SETTINGS
    .set(settings.clone())
    .expect("SETTINGS already set?");

  #[cfg(feature = "sql")]
  crate::db::init_db_pool(&settings.sql.db_url).await?;

  #[cfg(feature = "daily_challenge")]
  embed::spawn_cache_pruner();

  tokio::spawn(async {
    let _ = analysis::update_analysis_data().await;
  });

  tokio::spawn(async {
    loop {
      let _ = admin::update_oldest_user_inner().await;
      tokio::time::sleep(Duration::from_secs(17)).await;
    }
  });

  tokio::spawn(async {
    loop {
      let user_id: u64 = rand::random_range(3..40_000_000);
      let _ = admin::update_all_modes_for_user(user_id).await;
      tokio::time::sleep(Duration::from_secs(58)).await;
    }
  });

  let mut router = Router::new()
    .route("/", axum::routing::get(instrument_handler("index", index)))
    .route(
      "/a/v",
      axum::routing::post(instrument_handler("submit_event", analytics::submit_event)),
    )
    .route(
      "/a/z",
      axum::routing::post(instrument_handler(
        "submit_batch_events",
        analytics::submit_batch_events,
      )),
    )
    .route(
      "/a/stream",
      axum::routing::get(instrument_handler(
        "stream_analytics_events",
        analytics::stream_events,
      )),
    )
    .route(
      "/users/{user_id}/stats",
      axum::routing::get(instrument_handler("get_user_stats", get_user_stats)),
    )
    .route(
      "/users/{user_id}/hiscores",
      axum::routing::get(instrument_handler("get_hiscores", get_hiscores)),
    )
    .route(
      "/users/{user_id}/hiscores/v2",
      axum::routing::get(instrument_handler("get_hiscores_v2", get_hiscores_v2)),
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
        ))
        .layer(DefaultBodyLimit::max(64 * 1024)),
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
        "/daily-challenge/rankings/percent/100",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_percent_100",
          daily_challenge::get_daily_challenge_top_100_percent_rankings,
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
        "/daily-challenge/rankings/first-place",
        axum::routing::get(instrument_handler(
          "get_daily_challenge_rankings_first_place",
          daily_challenge::get_daily_challenge_first_place_rankings,
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
      .route(
        "/daily-challenge/embed",
        axum::routing::post(instrument_handler("create_embed", embed::create_embed)),
      )
      .route(
        "/daily-challenge/embed/preview",
        axum::routing::post(instrument_handler("preview_embed", embed::preview_embed)),
      )
      .route(
        "/daily-challenge/embed/{user_id}/{file}",
        axum::routing::get(instrument_handler("get_embed", embed::get_embed)),
      )
  }

  router = router
    .route(
      "/analysis/dataset",
      axum::routing::get(instrument_handler(
        "get_analysis_dataset",
        analysis::get_analysis_dataset,
      )),
    )
    .route(
      "/analysis/simulation-config",
      axum::routing::get(instrument_handler(
        "get_simulation_config",
        analysis::get_simulation_config,
      )),
    )
    .route(
      "/analysis/refresh",
      axum::routing::post(instrument_handler(
        "refresh_analysis_data",
        analysis::refresh_analysis_data,
      )),
    )
    .route(
      "/analysis/sanity/pp",
      axum::routing::get(instrument_handler("get_sanity_pp", analysis::get_sanity_pp)),
    )
    .route(
      "/analysis/sanity/decay",
      axum::routing::get(instrument_handler(
        "get_sanity_decay",
        analysis::get_sanity_decay,
      )),
    )
    .route(
      "/analysis/debug/plot",
      axum::routing::get(instrument_handler(
        "get_analysis_plot",
        analysis::get_analysis_plot,
      )),
    );

  #[cfg(feature = "sql")]
  {
    router = router
      .route(
        "/verify-best-plays",
        axum::routing::post(instrument_handler(
          "verify_best_plays",
          admin::verify_best_plays,
        )),
      )
      .route(
        "/maybe-undelete-user",
        axum::routing::post(instrument_handler(
          "maybe_undelete_user",
          admin::maybe_undelete_user,
        )),
      )
      .route(
        "/update-oldest-user",
        axum::routing::post(instrument_handler(
          "update_oldest_user",
          admin::update_oldest_user,
        )),
      )
      .route(
        "/users/{user}/update",
        axum::routing::post(instrument_handler(
          "osutrack_update",
          osutrack_update::osutrack_update,
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
