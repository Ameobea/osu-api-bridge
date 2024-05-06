use std::{fmt::Display, time::Instant};

use crate::{
  metrics::http_server, mods::build_mods_bitmap, oauth::REQWEST_CLIENT, server::APIError,
};

use axum::http::StatusCode;
use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

#[derive(Serialize)]
pub struct HiscoreV1 {
  pub beatmap_id: String,
  pub score_id: String,
  pub score: String,
  pub maxcombo: String,
  pub count50: String,
  pub count100: String,
  pub count300: String,
  pub countmiss: String,
  pub countkatu: String,
  pub countgeki: String,
  pub perfect: String,
  pub enabled_mods: String,
  pub is_classic: String,
  pub user_id: String,
  /// Formatted like "2020-10-23 13:17:20"
  pub date: String,
  pub rank: String,
  pub pp: String,
  pub replay_available: String,
}

pub type GetHiscoresV2Response = Vec<HiscoreV2>;

#[derive(Deserialize)]
pub struct HiscoreV2 {
  pub mods: Vec<Mod>,
  pub statistics: Statistics,
  pub beatmap_id: i64,
  pub best_id: Value,
  pub id: i64,
  pub rank: String,
  pub user_id: i64,
  pub accuracy: f64,
  pub build_id: Option<i64>,
  /// Formatted like "2014-12-18T20:59:27Z"
  pub ended_at: String,
  pub has_replay: bool,
  pub is_perfect_combo: bool,
  pub legacy_perfect: bool,
  pub legacy_score_id: Option<i64>,
  pub legacy_total_score: Option<i64>,
  pub max_combo: i64,
  pub passed: bool,
  pub pp: Option<f64>,
  pub ruleset_id: i64,
  pub started_at: Option<String>,
  pub total_score: i64,
  pub replay: bool,
  pub beatmap: Beatmap,
  pub user: User,
  pub weight: Weight,
}

fn unacronym_mods<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
  D: Deserializer<'de>,
{
  let mods: Vec<Mod> = Vec::deserialize(deserializer)?;
  Ok(mods.iter().map(|m| m.acronym.clone()).collect())
}

#[derive(Serialize, Deserialize)]
pub struct UserScoreOnBeatmap {
  pub ranked: bool,
  pub preserve: bool,
  pub processed: bool,
  #[serde(deserialize_with = "unacronym_mods")]
  pub mods: Vec<String>,
  pub statistics: Statistics,
  pub beatmap_id: i64,
  pub best_id: Value,
  pub id: i64,
  pub rank: String,
  pub user_id: i64,
  pub accuracy: f64,
  pub has_replay: bool,
  pub is_perfect_combo: bool,
  pub legacy_perfect: bool,
  pub legacy_score_id: Value,
  pub legacy_total_score: i64,
  pub max_combo: i64,
  pub passed: bool,
  pub pp: Option<f64>,
  pub ruleset_id: i64,
  pub started_at: Option<String>,
  pub ended_at: Option<String>,
  pub total_score: i64,
  pub replay: bool,
}

impl HiscoreV2 {
  pub fn into_v1(self) -> Result<HiscoreV1, String> {
    let ended_at = match chrono::DateTime::parse_from_rfc3339(&self.ended_at) {
      Ok(dt) => dt,
      Err(e) => {
        error!("Failed to parse date: {}", e);
        return Err("Failed to parse date".to_owned());
      },
    };
    let date = ended_at.format("%Y-%m-%d %H:%M:%S").to_string();

    Ok(HiscoreV1 {
      beatmap_id: self.beatmap.id.to_string(),
      score_id: self
        .legacy_score_id
        .filter(|id| *id > 0)
        .unwrap_or(self.id)
        .to_string(),
      score: self
        .legacy_total_score
        .filter(|score| *score > 0)
        .unwrap_or(self.total_score)
        .to_string(),
      maxcombo: self.max_combo.to_string(),
      count50: self.statistics.meh.unwrap_or(0).to_string(),
      count100: self.statistics.ok.unwrap_or(0).to_string(),
      count300: self.statistics.great.unwrap_or(0).to_string(),
      countmiss: self.statistics.miss.unwrap_or(0).to_string(),
      countkatu: self.statistics.good.unwrap_or(0).to_string(),
      countgeki: self.statistics.perfect.unwrap_or(0).to_string(),
      perfect: if self.is_perfect_combo { "1" } else { "0" }.to_owned(),
      enabled_mods: build_mods_bitmap(&self.mods).to_string(),
      is_classic: if self.mods.iter().any(|m| m.acronym == "CL") {
        "1"
      } else {
        "0"
      }
      .to_owned(),
      user_id: self.user.id.to_string(),
      date,
      rank: self.rank,
      pp: self.pp.unwrap_or(0.).to_string(),
      replay_available: if self.has_replay { "1" } else { "0" }.to_owned(),
    })
  }
}

#[derive(Deserialize)]
pub struct Mod {
  pub acronym: String,
}

#[derive(Serialize, Deserialize)]
pub struct Statistics {
  pub ok: Option<i64>,
  pub meh: Option<i64>,
  pub miss: Option<i64>,
  pub great: Option<i64>,
  pub perfect: Option<i64>,
  pub good: Option<i64>,
  pub large_bonus: Option<i64>,
  pub small_bonus: Option<i64>,
}

#[derive(Deserialize)]
pub struct Beatmap {
  pub beatmapset_id: i64,
  pub id: i64,
  pub mode: String,
}

#[derive(Deserialize)]
pub struct Beatmapset {
  pub id: i64,
}

#[derive(Deserialize)]
pub struct Covers {
  pub cover: String,
  #[serde(rename = "cover@2x")]
  pub cover_2x: String,
  pub card: String,
  #[serde(rename = "card@2x")]
  pub card_2x: String,
  pub list: String,
  #[serde(rename = "list@2x")]
  pub list_2x: String,
  pub slimcover: String,
  #[serde(rename = "slimcover@2x")]
  pub slimcover_2x: String,
}

#[derive(Deserialize)]
pub struct User {
  pub id: u64,
}

#[derive(Deserialize)]
pub struct Weight {
  pub percentage: f64,
  pub pp: Option<f64>,
}

pub enum Ruleset {
  Osu,
  Taiko,
  Ctb,
  Mania,
}

impl Display for Ruleset {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", match self {
      Ruleset::Osu => "osu",
      Ruleset::Taiko => "taiko",
      Ruleset::Ctb => "fruits",
      Ruleset::Mania => "mania",
    })
  }
}

impl<'de> Deserialize<'de> for Ruleset {
  fn deserialize<D>(deserializer: D) -> Result<Ruleset, D::Error>
  where
    D: Deserializer<'de>,
  {
    let s = String::deserialize(deserializer)?;
    Ok(match s.as_str() {
      "osu" | "0" => Ruleset::Osu,
      "taiko" | "1" => Ruleset::Taiko,
      "fruits" | "2" => Ruleset::Ctb,
      "mania" | "3" => Ruleset::Mania,
      _ => {
        return Err(serde::de::Error::custom(format!("Unknown ruleset: {}", s)));
      },
    })
  }
}

async fn get_auth_header() -> Result<String, APIError> {
  crate::oauth::get_auth_header().await.map_err(|err| {
    error!("Failed to get auth header: {}", err);
    http_server::osu_api_requests_failed_total("fetch_user_hiscores", 0).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to get auth header".to_owned(),
    }
  })
}

async fn make_osu_api_request(
  url: &str,
  endpoint_name: &'static str,
  method: Method,
) -> Result<String, APIError> {
  let auth_header = get_auth_header().await?;

  let now = Instant::now();
  let res = REQWEST_CLIENT
    .request(method, url)
    .header("Content-Type", "application/json")
    .header("x-api-version", "20220705")
    .header("Accept", "application/json")
    .header("Authorization", auth_header)
    .send()
    .await
    .map_err(|err| {
      error!("Error making osu! API request for {endpoint_name} to {url}: {err}");
      http_server::osu_api_requests_failed_total(endpoint_name, 0).inc();
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to make osu! API request".to_owned(),
      }
    })?;
  let elapsed = now.elapsed();
  http_server::osu_api_response_time_seconds(endpoint_name).observe(elapsed.as_nanos() as u64);

  let status_code = res.status();
  res.text().await.map_err(|err| {
    error!(
      ?status_code,
      ?endpoint_name,
      "Failed to read osu! API response: {err}"
    );
    http_server::osu_api_requests_failed_total(endpoint_name, status_code.as_u16()).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to response from osu! API".to_owned(),
    }
  })
}

// curl --request GET \
//     --get "https://osu.ppy.sh/api/v2/users/493378/scores/best?include_fails=0&legacy_only=false&mode=osu&limit=51&offset=5" \
//     --header "Content-Type: application/json" --header "x-api-version: 20220705" \
//     --header "Accept: application/json" --header "Authorization: Bearer <token>"
pub async fn fetch_user_hiscores(
  user_id: u64,
  mode: Ruleset,
  limit: Option<u8>,
  offset: Option<u8>,
) -> Result<GetHiscoresV2Response, APIError> {
  http_server::osu_api_requests_total("fetch_user_hiscores").inc();

  let limit = limit.unwrap_or(100);
  let offset = offset.unwrap_or(0);

  let proxy_url = format!(
    "https://osu.ppy.sh/api/v2/users/{user_id}/scores/best?include_fails=0&legacy_only=false&mode={mode}&limit={limit}&offset={offset}",
  );
  let res_text = make_osu_api_request(&proxy_url, "fetch_user_hiscores", Method::GET).await?;

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  match serde_path_to_error::deserialize(deserializer) {
    Ok(hiscores) => Ok(hiscores),
    Err(err) => {
      error!("Failed to parse user hiscores response; res: {res_text}; err: {err}");
      http_server::osu_api_requests_failed_total("fetch_user_hiscores", 200).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user hiscores response".to_owned(),
      })
    },
  }
}

#[derive(Deserialize)]
struct FetchAllUserScoresForBeatmapRes {
  scores: Vec<UserScoreOnBeatmap>,
}

pub async fn fetch_all_user_scores_for_beatmap(
  user_id: u64,
  beatmap_id: u64,
  mode: Ruleset,
) -> Result<Vec<UserScoreOnBeatmap>, APIError> {
  let endpoint_name = "fetch_all_user_scores_for_beatmap";
  let proxy_url = format!("https://osu.ppy.sh/api/v2/beatmaps/{beatmap_id}/scores/users/{user_id}/all?legacy_only=0&mode={mode}");
  let res_text = make_osu_api_request(&proxy_url, endpoint_name, Method::GET).await?;

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  match serde_path_to_error::deserialize::<_, FetchAllUserScoresForBeatmapRes>(deserializer) {
    Ok(res) => Ok(res.scores),
    Err(err) => {
      error!("Failed to parse user score response; res: {res_text}; err: {err}");
      http_server::osu_api_requests_failed_total(endpoint_name, 200).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user score response".to_owned(),
      })
    },
  }
}

pub async fn fetch_user_id(username: &str, mode: Ruleset) -> Result<u64, APIError> {
  let endpoint_name = "get_user_id";
  let proxy_url = format!("https://osu.ppy.sh/api/v2/users/{username}/{mode}?key=username");
  let res_text = make_osu_api_request(&proxy_url, endpoint_name, Method::GET).await?;

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  match serde_path_to_error::deserialize::<_, User>(deserializer) {
    Ok(user) => Ok(user.id),
    Err(err) => {
      error!("Failed to parse user response; res: {res_text}; err: {err}");
      http_server::osu_api_requests_failed_total(endpoint_name, 200).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user response".to_owned(),
      })
    },
  }
}
