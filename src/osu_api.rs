use std::{fmt::Display, time::Instant};

use axum::http::StatusCode;
use reqwest::Method;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
  metrics::http_server, mods::build_mods_bitmap, oauth::REQWEST_CLIENT, server::APIError,
};

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

#[derive(Serialize, Deserialize)]
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
  #[serde(skip_serializing)]
  pub beatmap: Beatmap,
  #[serde(skip_serializing)]
  pub user: User,
}

impl HiscoreV2 {
  /// Builds the pkey used by the `beatmap_difficulties` table
  pub fn build_score_id(&self) -> String {
    let order = ["DT", "EZ", "FL", "HR", "HT"];

    let mut combined_mods = String::new();
    for m in order {
      for acronym in &self.mods {
        if acronym.acronym == m {
          combined_mods.push_str(m);
        }
      }
    }

    format!("{}_{}", self.beatmap_id, combined_mods)
  }
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Mod {
  pub acronym: String,
  pub settings: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Clone, Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct Beatmapset {
  pub artist: String,
  pub artist_unicode: String,
  pub covers: Covers,
  pub creator: String,
  pub favourite_count: i64,
  pub hype: Value,
  pub id: i64,
  pub nsfw: bool,
  pub offset: i64,
  pub play_count: i64,
  pub preview_url: String,
  pub source: String,
  pub spotlight: bool,
  pub status: String,
  pub title: String,
  pub title_unicode: String,
  pub track_id: Value,
  pub user_id: i64,
  pub video: bool,
}

#[derive(Serialize, Deserialize)]
pub struct Beatmap {
  pub beatmapset_id: Option<i64>,
  pub difficulty_rating: f32,
  pub id: i64,
  pub mode: String,
  pub status: String,
  pub total_length: i64,
  pub user_id: i64,
  pub version: String,
  pub beatmapset: Option<Beatmapset>,
}

#[derive(sqlx::FromRow, Serialize)]
pub struct OsutrackDbBeatmap {
  pub beatmapset_id: i64,
  pub beatmap_id: i64,
  pub approved: i64,
  pub approved_date: Option<chrono::NaiveDateTime>,
  pub last_update: chrono::NaiveDateTime,
  pub total_length: i64,
  pub hit_length: i64,
  pub version: String,
  pub artist: String,
  pub title: String,
  pub creator: String,
  pub bpm: i64,
  pub source: String,
  pub difficultyrating: f64,
  pub diff_size: f64,
  pub diff_overall: f64,
  pub diff_approach: f64,
  pub diff_drain: f64,
  pub mode: i64,
}

#[derive(Clone, sqlx::FromRow, Serialize)]
pub struct BeatmapDifficulties {
  pub score_id: String,
  pub difficulty_aim: f64,
  pub difficulty_speed: f64,
  pub difficulty_flashlight: f64,
  pub speed_note_count: f64,
  pub slider_factor: f64,
  pub stars: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct User {
  pub id: u64,
  pub username: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Ruleset {
  Osu,
  Taiko,
  Ctb,
  Mania,
}

impl Ruleset {
  pub const ALL: [Ruleset; 4] = [Ruleset::Osu, Ruleset::Taiko, Ruleset::Ctb, Ruleset::Mania];

  pub fn mode_value(self) -> i8 {
    match self {
      Ruleset::Osu => 0,
      Ruleset::Taiko => 1,
      Ruleset::Ctb => 2,
      Ruleset::Mania => 3,
    }
  }
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

async fn get_auth_header(endpoint_name: &'static str) -> Result<String, APIError> {
  crate::oauth::get_auth_header().await.map_err(|err| {
    error!("Failed to get auth header: {err:?}");
    http_server::osu_api_requests_failed_total(endpoint_name, 0).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to get auth header".to_owned(),
    }
  })
}

#[cfg(feature = "sql")]
async fn get_user_auth_header(endpoint_name: &'static str) -> Result<String, APIError> {
  crate::oauth::get_user_auth_header().await.map_err(|err| {
    error!("Failed to get user auth header: {err:?}");
    http_server::osu_api_requests_failed_total(endpoint_name, 0).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to get user auth header".to_owned(),
    }
  })
}

async fn make_osu_api_request_inner(
  url: &str,
  endpoint_name: &'static str,
  method: Method,
  auth_header: String,
) -> Result<String, APIError> {
  http_server::osu_api_requests_total(endpoint_name).inc();

  let now = Instant::now();
  let res = REQWEST_CLIENT
    .request(method, url)
    .header("Content-Type", "application/json")
    .header("x-api-version", "20240923")
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

async fn make_osu_api_request(
  url: &str,
  endpoint_name: &'static str,
  method: Method,
) -> Result<String, APIError> {
  let auth_header = get_auth_header(endpoint_name).await?;
  make_osu_api_request_inner(url, endpoint_name, method, auth_header).await
}

#[cfg(feature = "sql")]
async fn make_osu_user_api_request(
  url: &str,
  endpoint_name: &'static str,
  method: Method,
) -> Result<String, APIError> {
  let auth_header = get_user_auth_header(endpoint_name).await?;
  make_osu_api_request_inner(url, endpoint_name, method, auth_header).await
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
  match serde_path_to_error::deserialize::<_, FetchUserRes>(deserializer) {
    Ok(FetchUserRes::User(user)) => Ok(user.id),
    Ok(FetchUserRes::UserNotFound { .. }) => {
      warn!("User not found: {username}; res: {res_text}");
      Err(APIError {
        status: StatusCode::NOT_FOUND,
        message: "User not found".to_owned(),
      })
    },
    Err(err) => {
      error!("Failed to parse get user ID response; res: {res_text}; err: {err}");
      http_server::osu_api_requests_failed_total(endpoint_name, 200).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user response".to_owned(),
      })
    },
  }
}

#[derive(Deserialize, PartialEq, Debug)]
#[serde(untagged)]
enum FetchUserRes {
  User(User),
  UserNotFound { error: Option<serde_json::Value> },
}

#[test]
fn fetch_user_success_res_parse() {
  let res_text = r#"{"id":4093752,"username":"ameo"}"#;
  let deserializer = &mut serde_json::Deserializer::from_str(res_text);
  let res: FetchUserRes = serde_path_to_error::deserialize(deserializer).unwrap();
  assert_eq!(
    res,
    FetchUserRes::User(User {
      id: 4093752,
      username: "ameo".to_owned()
    })
  );
}

#[test]
fn fetch_user_not_found_res_parse() {
  let res_text = r#"{"error":null}"#;
  let deserializer = &mut serde_json::Deserializer::from_str(res_text);
  let res: FetchUserRes = serde_path_to_error::deserialize(deserializer).unwrap();
  assert_eq!(res, FetchUserRes::UserNotFound { error: None });
}

pub async fn fetch_username(user_id: u64) -> Result<Option<String>, APIError> {
  let endpoint_name = "get_username";
  let proxy_url = format!("https://osu.ppy.sh/api/v2/users/{user_id}");
  let res_text = make_osu_api_request(&proxy_url, endpoint_name, Method::GET).await?;

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  match serde_path_to_error::deserialize::<_, FetchUserRes>(deserializer) {
    Ok(FetchUserRes::User(user)) => Ok(Some(user.username)),
    Ok(FetchUserRes::UserNotFound { .. }) => {
      warn!("User not found: {user_id}; res: {res_text}");
      Ok(None)
    },
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

#[cfg(feature = "daily_challenge")]
pub mod daily_challenge {
  use chrono::DateTime;
  use reqwest::StatusCode;
  use serde::{Deserialize, Serialize};

  use crate::server::APIError;

  use super::Beatmap;

  #[derive(Serialize, Deserialize)]
  pub struct DailyChallengeScore {
    pub preserve: bool,
    pub processed: bool,
    pub ranked: bool,
    pub mods: Vec<super::Mod>,
    pub statistics: super::Statistics,
    pub total_score_without_mods: i64,
    pub beatmap_id: i64,
    pub id: i64,
    pub rank: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub user_id: i64,
    pub accuracy: f32,
    pub build_id: i64,
    pub ended_at: DateTime<chrono::Utc>,
    pub has_replay: bool,
    pub is_perfect_combo: bool,
    pub max_combo: usize,
    pub passed: bool,
    pub pp: Option<f32>,
    pub ruleset_id: usize,
    pub started_at: DateTime<chrono::Utc>,
    pub total_score: usize,
    pub replay: bool,
  }

  #[derive(Serialize)]
  pub struct NewDailyChallengeDescriptor {
    /// like 20240420
    pub day_id: usize,
    pub room_id: i64,
    pub playlist_id: i64,
    pub current_playlist_item: CurrentPlaylistItem,
  }

  fn get_day_id(starts_at: chrono::DateTime<chrono::Utc>) -> Result<usize, APIError> {
    let date = starts_at.format("%Y%m%d").to_string();
    date.parse().map_err(|err| {
      error!("Failed to parse date: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to daily challenge room start date into day ID".to_owned(),
      }
    })
  }

  #[derive(Serialize, Deserialize)]
  pub struct CurrentPlaylistItem {
    pub id: i64,
    pub room_id: i64,
    pub allowed_mods: Vec<serde_json::Map<String, serde_json::Value>>,
    pub required_mods: Vec<serde_json::Map<String, serde_json::Value>>,
    pub beatmap: Beatmap,
  }

  #[derive(Deserialize)]
  struct DifficultyRange {
    // min: f32,
    // max: f32,
  }

  #[derive(Deserialize)]
  struct Room {
    pub id: i64,
    pub current_playlist_item: CurrentPlaylistItem,
    // pub difficulty_range: DifficultyRange,
    pub starts_at: chrono::DateTime<chrono::Utc>,
  }

  /// If `active` is true, only the current daily challenge is returned.  Otherwise, only past/ended
  /// daily challenges are returned.
  pub async fn get_daily_challenge_descriptors(
    active: bool,
  ) -> Result<Vec<NewDailyChallengeDescriptor>, super::APIError> {
    let endpoint_name = "get_daily_challenge_room_id";
    let url = format!(
      "https://osu.ppy.sh/api/v2/rooms?category=daily_challenge&mode={}",
      if active { "active" } else { "ended" }
    );
    let res_text =
      super::make_osu_user_api_request(&url, endpoint_name, reqwest::Method::GET).await?;

    type GetDailyChallengeRoomIdsResponse = Vec<Room>;

    let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
    match serde_path_to_error::deserialize::<_, GetDailyChallengeRoomIdsResponse>(deserializer) {
      Ok(rooms) => Ok(
        rooms
          .into_iter()
          .map(|r| {
            Ok(NewDailyChallengeDescriptor {
              day_id: get_day_id(r.starts_at)?,
              room_id: r.id,
              playlist_id: r.current_playlist_item.id,
              current_playlist_item: r.current_playlist_item,
            })
          })
          .collect::<Result<_, _>>()?,
      ),
      Err(err) => {
        error!("Failed to parse daily challenge room response; res: {res_text}; err: {err}");
        super::http_server::osu_api_requests_failed_total(endpoint_name, 200).inc();
        Err(super::APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to parse daily challenge room response".to_owned(),
        })
      },
    }
  }

  struct FetchDailyScoresPage {
    pub scores: Vec<DailyChallengeScore>,
    pub cursor_string: Option<String>,
  }

  // https://osu.ppy.sh/api/v2/rooms/898511/playlist/9530696/scores?limit=50&cursor_string=<...>
  async fn fetch_daily_challenge_scores_page(
    ids: &NewDailyChallengeDescriptor,
    cursor_string: Option<&str>,
  ) -> Result<FetchDailyScoresPage, super::APIError> {
    let endpoint_name = "fetch_daily_challenge_scores";
    let proxy_url = format!(
      "https://osu.ppy.sh/api/v2/rooms/{}/playlist/{}/scores?limit=50&cursor_string={}",
      ids.room_id,
      ids.playlist_id,
      cursor_string.unwrap_or("")
    );
    let res_text =
      super::make_osu_user_api_request(&proxy_url, endpoint_name, reqwest::Method::GET).await?;

    #[derive(Deserialize)]
    struct ScoresResponse {
      pub scores: Vec<DailyChallengeScore>,
      pub cursor_string: Option<String>,
    }

    let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
    match serde_path_to_error::deserialize::<_, ScoresResponse>(deserializer) {
      Ok(res) => Ok(FetchDailyScoresPage {
        scores: res.scores,
        cursor_string: res.cursor_string,
      }),
      Err(err) => {
        error!("Failed to parse daily challenge scores response; res: {res_text}; err: {err}");
        super::http_server::osu_api_requests_failed_total(endpoint_name, 200).inc();
        Err(super::APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to parse daily challenge scores response".to_owned(),
        })
      },
    }
  }

  /// Fetch all scores for a daily challenge, pulling all pages.
  pub async fn fetch_daily_challenge_scores(
    ids: &NewDailyChallengeDescriptor,
  ) -> Result<Vec<DailyChallengeScore>, super::APIError> {
    let mut all_scores = Vec::new();
    let mut cursor_string = None;
    let mut page_ix = 0usize;
    loop {
      info!("Fetching daily challenge scores page {page_ix}");
      let page = fetch_daily_challenge_scores_page(ids, cursor_string.as_deref()).await?;
      let page_score_count = page.scores.len();
      all_scores.extend(page.scores);
      if page.cursor_string.is_none() || page_score_count < 50 {
        break;
      }

      cursor_string = page.cursor_string;
      tokio::time::sleep(std::time::Duration::from_secs(1)).await;
      page_ix += 1;
    }

    Ok(all_scores)
  }
}
