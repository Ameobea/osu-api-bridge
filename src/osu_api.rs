use std::{fmt::Display, time::Instant};

use crate::{metrics::http_server, oauth::REQWEST_CLIENT, server::APIError};

use axum::http::StatusCode;
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
  pub ranked: bool,
  pub preserve: bool,
  pub maximum_statistics: MaximumStatistics,
  pub mods: Vec<Mod>,
  pub statistics: Statistics,
  pub beatmap_id: i64,
  pub best_id: Value,
  pub id: i64,
  pub rank: String,
  #[serde(rename = "type")]
  pub score_type: String,
  pub user_id: i64,
  pub accuracy: f64,
  pub build_id: Option<i64>,
  /// Formatted like "2014-12-18T20:59:27Z"
  pub ended_at: String,
  pub has_replay: bool,
  pub is_perfect_combo: bool,
  pub legacy_perfect: bool,
  pub legacy_score_id: Option<i64>,
  pub legacy_total_score: i64,
  pub max_combo: i64,
  pub passed: bool,
  pub pp: Option<f64>,
  pub ruleset_id: i64,
  pub started_at: Option<String>,
  pub total_score: i64,
  pub replay: bool,
  // pub current_user_attributes: CurrentUserAttributes,
  pub beatmap: Beatmap,
  // pub beatmapset: Beatmapset,
  pub user: User,
  pub weight: Weight,
}

fn build_mods_bitmap(mods: &[Mod]) -> u32 {
  let mut bitmap = 0;
  for m in mods {
    bitmap |= match m.acronym.as_str() {
      "NF" => 1 << 0,
      "EZ" => 1 << 1,
      "TD" => 1 << 2,
      "HD" => 1 << 3,
      "HR" => 1 << 4,
      "SD" => 1 << 5,
      "DT" => 1 << 6,
      "RX" => 1 << 7,
      "HT" => 1 << 8,
      // NC also applies DT's bit
      "NC" => (1 << 9) | (1 << 6),
      "FL" => 1 << 10,
      "AT" => 1 << 11,
      "SO" => 1 << 12,
      "AP" => 1 << 13,
      // PF also applies SD's bit
      "PF" => (1 << 14) | (1 << 5),
      "4K" => 1 << 15,
      "5K" => 1 << 16,
      "6K" => 1 << 17,
      "7K" => 1 << 18,
      "8K" => 1 << 19,
      "FI" => 1 << 20,
      "RD" => 1 << 21,
      "CN" => 1 << 22,
      "TP" => 1 << 23,
      "9K" => 1 << 24,
      "CO" => 1 << 25,
      "1K" => 1 << 26,
      "3K" => 1 << 27,
      "2K" => 1 << 28,
      "V2" => 1 << 29,
      "MR" => 1 << 30,
      "CL" => 0,
      _ => {
        error!("Unknown mod: {}", m.acronym);
        0
      },
    };
  }
  bitmap
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
      score_id: self.legacy_score_id.unwrap_or(self.id).to_string(),
      score: self.legacy_total_score.to_string(),
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

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct MaximumStatistics {
  pub great: Option<i64>,
  pub legacy_combo_increase: Option<i64>,
  pub ignore_hit: Option<i64>,
  pub large_bonus: Option<i64>,
  pub small_bonus: Option<i64>,
  pub large_tick_hit: Option<i64>,
  pub slider_tail_hit: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct Mod {
  pub acronym: String,
}

#[derive(Default, Debug, Clone, PartialEq, Deserialize)]
pub struct Statistics {
  pub ok: Option<i64>,
  pub meh: Option<i64>,
  pub miss: Option<i64>,
  pub great: Option<i64>,
  pub perfect: Option<i64>,
  pub good: Option<i64>,
  // pub ignore_hit: Option<i64>,
  // pub ignore_miss: Option<i64>,
  pub large_bonus: Option<i64>,
  pub small_bonus: Option<i64>,
  // pub large_tick_hit: Option<i64>,
  // pub slider_tail_hit: Option<i64>,
}

#[derive(Deserialize)]
pub struct CurrentUserAttributes {
  pub pin: Value,
}

#[derive(Deserialize)]
pub struct Beatmap {
  pub beatmapset_id: i64,
  // pub difficulty_rating: f64,
  pub id: i64,
  pub mode: String,
  // pub status: String,
  // pub total_length: i64,
  // pub user_id: i64,
  // pub version: String,
  // pub accuracy: f64,
  // pub ar: f64,
  // pub bpm: f64,
  // pub convert: bool,
  // pub count_circles: i64,
  // pub count_sliders: i64,
  // pub count_spinners: i64,
  // pub cs: f64,
  // pub deleted_at: Value,
  // pub drain: f64,
  // pub hit_length: i64,
  // pub is_scoreable: bool,
  // pub last_updated: String,
  // pub mode_int: i64,
  // pub passcount: i64,
  // pub playcount: i64,
  // pub ranked: i64,
  // pub url: String,
  // pub checksum: String,
}

#[derive(Deserialize)]
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
  pub track_id: Option<i64>,
  pub user_id: i64,
  pub video: bool,
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
  // pub avatar_url: String,
  // pub country_code: String,
  // pub default_group: String,
  pub id: u64,
  // pub is_active: bool,
  // pub is_bot: bool,
  // pub is_deleted: bool,
  // pub is_online: bool,
  // pub is_supporter: bool,
  // pub last_visit: String,
  // pub pm_friends_only: bool,
  // pub profile_colour: Value,
  // pub username: String,
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

// curl --request GET \
//     --get "https://osu.ppy.sh/api/v2/users/493378/scores/best?include_fails=0&legacy_only=false&mode=osu&limit=51&offset=5" \
//     --header "Content-Type: application/json" --header "x-api-version: 20220705" \
//     --header "Accept: application/json" --header "Authorization: Bearer <token>"
pub(crate) async fn fetch_user_hiscores(
  user_id: u64,
  mode: Ruleset,
  limit: Option<u8>,
  offset: Option<u8>,
) -> Result<GetHiscoresV2Response, APIError> {
  let auth_header = crate::oauth::get_auth_header().await.map_err(|err| {
    error!("Failed to get auth header: {}", err);
    http_server::osu_api_requests_failed_total("fetch_user_hiscores", 0).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to get auth header".to_owned(),
    }
  })?;

  http_server::osu_api_requests_total("fetch_user_hiscores").inc();
  let now = Instant::now();
  let limit = limit.unwrap_or(100);
  let offset = offset.unwrap_or(0);
  let res = REQWEST_CLIENT
    .get(&format!(
      "https://osu.ppy.sh/api/v2/users/{user_id}/scores/best?include_fails=0&legacy_only=false&mode={mode}&limit={limit}&offset={offset}",
    ))
    .header("Content-Type", "application/json")
    .header("x-api-version", "20220705")
    .header("Accept", "application/json")
    .header("Authorization", auth_header)
    .send()
    .await
    .map_err(|err| {
        error!("Failed to fetch user hiscores: {}", err);
        http_server::osu_api_requests_failed_total("fetch_user_hiscores", err.status().map(|s| s.as_u16()).unwrap_or(0)).inc();
        APIError {
          status: StatusCode::INTERNAL_SERVER_ERROR,
          message: "Failed to fetch user hiscores".to_owned(),
        }
    })?;
  let status_code = res.status();
  let res_text = res.text().await.map_err(|err| {
    error!(
      ?status_code,
      "Failed to read user hiscores response: {}", err
    );
    http_server::osu_api_requests_failed_total("fetch_user_hiscores", status_code.as_u16()).inc();
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to read user hiscores response".to_owned(),
    }
  })?;

  let elapsed = now.elapsed();
  http_server::osu_api_response_time_seconds("fetch_user_hiscores")
    .observe(elapsed.as_nanos() as u64);

  if !status_code.is_success() {
    error!(
      ?status_code,
      "Failed to fetch user hiscores; status: {status_code}; res: {res_text}"
    );
    http_server::osu_api_requests_failed_total("fetch_user_hiscores", status_code.as_u16()).inc();
    return Err(APIError {
      status: axum::http::StatusCode::from_u16(status_code.as_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
      message: "Failed to fetch user hiscores".to_owned(),
    });
  }

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  match serde_path_to_error::deserialize(deserializer) {
    Ok(hiscores) => Ok(hiscores),
    Err(err) => {
      error!(
        ?status_code,
        "Failed to parse user hiscores response; res: {res_text}; err: {err}"
      );
      http_server::osu_api_requests_failed_total("fetch_user_hiscores", status_code.as_u16()).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user hiscores response".to_owned(),
      })
    },
  }
}
