use std::{path::PathBuf, sync::Arc, time::Duration};

use axum::{
  extract::Path,
  http::{
    header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH},
    HeaderMap, HeaderValue, StatusCode,
  },
  response::{IntoResponse, Response},
  Json,
};
use base64::Engine;
use chrono::{Datelike, NaiveDate};
use fxhash::FxHashMap;
use lazy_static::lazy_static;
use moka::sync::Cache;
use resvg::{tiny_skia, usvg};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
  daily_challenge::{
    compute_user_daily_challenge_stats, latest_day_id, user_month_percentiles,
    DailyChallengeUserStats, Histogram,
  },
  APIError, SETTINGS,
};
use crate::{db::db_pool, osu_api::Mod};

lazy_static! {
  static ref FONTDB: Arc<usvg::fontdb::Database> = {
    let mut db = usvg::fontdb::Database::new();
    db.load_font_data(include_bytes!("../../assets/fonts/IBMPlexSans-Regular.ttf").to_vec());
    db.load_font_data(include_bytes!("../../assets/fonts/IBMPlexSans-SemiBold.ttf").to_vec());
    Arc::new(db)
  };
  static ref AVATAR_CACHE: Cache<u64, Option<String>> = Cache::builder()
    .max_capacity(10_000)
    .time_to_live(Duration::from_secs(6 * 3600))
    .build();
  static ref USERNAME_CACHE: Cache<u64, String> = Cache::builder()
    .max_capacity(10_000)
    .time_to_live(Duration::from_secs(600))
    .build();
  static ref USER_STATS_CACHE: Cache<(u64, usize), Arc<DailyChallengeUserStats>> = Cache::builder()
    .max_capacity(10_000)
    .time_to_live(Duration::from_secs(120))
    .build();
  static ref HTTP: reqwest::Client = reqwest::Client::new();
  // SVGs loaded via <img> can't fetch external resources, so the font must be inlined as a data URI
  // (the PNG path renders with FONTDB and ignores this @font-face)
  static ref FONT_FACE_STYLE: String = {
    let enc = base64::engine::general_purpose::STANDARD;
    // Latin subset keeps the self-contained SVG small; PNGs use the full TTFs in FONTDB
    let reg = enc.encode(include_bytes!("../../assets/fonts/IBMPlexSans-Regular.subset.woff2"));
    let semi = enc.encode(include_bytes!("../../assets/fonts/IBMPlexSans-SemiBold.subset.woff2"));
    format!(
      r#"<style>@font-face{{font-family:'IBM Plex Sans';font-style:normal;font-weight:400;src:url(data:font/woff2;base64,{reg}) format('woff2');}}@font-face{{font-family:'IBM Plex Sans';font-style:normal;font-weight:600;src:url(data:font/woff2;base64,{semi}) format('woff2');}}</style>"#
    )
  };
}

const FONT_FAMILY: &str = "IBM Plex Sans";
const CACHE_CONTROL_VALUE: &str = "public, max-age=3600";
const MAX_EMBED_STATS: usize = 24;
const MIN_PNG_SCALE: f32 = 1.;
const MAX_PNG_SCALE: f32 = 4.;
const MAX_PNG_DIMENSION: u32 = 4096;
const MAX_PNG_PIXELS: u64 = 12_000_000;
const MAX_AVATAR_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StatKey {
  Participation,
  CurrentDailyStreak,
  BestDailyStreak,
  CurrentWeeklyStreak,
  BestWeeklyStreak,
  GlobalRank,
  TotalScore,
  BestPlacementRank,
  BestPlacementPercentile,
  BestPlacementScore,
  BestPlacementPp,
  Top50Count,
  Top10Count,
  Top1Count,
  FirstPlaceCount,
  Top1Streak,
  Top10Streak,
  Top50Streak,
  MostUsedMods,
}

/// Optional panel to the right of the stats list; anything but `None` engages the two-column
/// layout.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Feature {
  None,
  Calendar,
  ScoreHistogram,
  TimeOfDayHistogram,
  HeroStat(StatKey), // serializes as {"hero_stat":"global_rank"}
}

fn default_feature() -> Feature { Feature::Calendar }

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct EmbedColors {
  pub background: String,
  pub text: String,
  pub secondary_text: String,
  pub border: String,
}

impl Default for EmbedColors {
  fn default() -> Self {
    EmbedColors {
      background: "#161616".into(),
      text: "#fefefe".into(),
      secondary_text: "#b3b3b3".into(),
      border: "#22282a".into(),
    }
  }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct EmbedConfig {
  pub stats: Vec<StatKey>,
  #[serde(default)]
  pub colors: EmbedColors,
  #[serde(default = "default_true")]
  pub rainbow_full_streak: bool,
  #[serde(default = "default_true")]
  pub show_avatar: bool,
  #[serde(default = "default_true")]
  pub show_header: bool,
  #[serde(default)]
  pub scale: Option<f32>,
  #[serde(default = "default_feature")]
  pub feature: Feature,
}

fn default_true() -> bool { true }

#[derive(Deserialize)]
pub(crate) struct EmbedRequest {
  user_id: u64,
  config: EmbedConfig,
}

fn bad_request(message: impl Into<String>) -> APIError {
  APIError {
    status: StatusCode::BAD_REQUEST,
    message: message.into(),
  }
}

fn validate_color(s: &str) -> bool {
  s.len() == 7 && s.starts_with('#') && s.as_bytes()[1..].iter().all(|b| b.is_ascii_hexdigit())
}

fn validate_config(config: &EmbedConfig) -> Result<(), APIError> {
  if config.stats.len() > MAX_EMBED_STATS {
    return Err(bad_request(format!(
      "embed stats cannot exceed {MAX_EMBED_STATS} entries"
    )));
  }
  for color in [
    &config.colors.background,
    &config.colors.text,
    &config.colors.secondary_text,
    &config.colors.border,
  ] {
    if !validate_color(color) {
      return Err(bad_request("embed colors must be #rrggbb values"));
    }
  }
  if let Some(scale) = config.scale {
    if !scale.is_finite() || !(MIN_PNG_SCALE..=MAX_PNG_SCALE).contains(&scale) {
      return Err(bad_request(format!(
        "embed scale must be between {MIN_PNG_SCALE} and {MAX_PNG_SCALE}"
      )));
    }
  }
  Ok(())
}

fn validate_hash(hash: &str) -> bool {
  hash.len() == 10 && hash.bytes().all(|b| b.is_ascii_alphanumeric())
}

fn db_user_id(user_id: u64) -> Result<i64, APIError> {
  if user_id > i32::MAX as u64 {
    return Err(bad_request("invalid user_id"));
  }
  Ok(user_id as i64)
}

/// user_id is intentionally excluded so identical styling hashes the same for everyone; rows are
/// keyed by (hash, user_id)
fn config_hash(config: &EmbedConfig) -> String {
  let canonical = serde_json::to_string(config).unwrap();
  let digest = Sha256::digest(canonical.as_bytes());
  let n = u128::from_be_bytes(digest[..16].try_into().unwrap());
  format!("{:0>10}", base62::encode(n))
    .chars()
    .take(10)
    .collect()
}

/// bump on any renderer change; folded into cache filenames + ETags so old renders are swept by
/// `prune_siblings`
const RENDER_VERSION: u32 = 10;

const W: f32 = 480.; // single-column width (no feature panel)
const PAD_X: f32 = 14.;
const PAD_Y: f32 = 14.;
const AV: f32 = 56.;
const ROW_H: f32 = 30.;
const HEADER_GAP: f32 = 14.;

// two-column geometry (when a feature panel is present)
const COL_GAP: f32 = 24.;
const STATS_COL_W: f32 = 300.;
const PANEL_X: f32 = PAD_X + STATS_COL_W + COL_GAP;

const RAINBOW_DEF: &str = r##"<linearGradient id="rainbow" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stop-color="#ff0000"/><stop offset="0.14" stop-color="#ffa500"/><stop offset="0.28" stop-color="#ffff00"/><stop offset="0.43" stop-color="#008000"/><stop offset="0.57" stop-color="#0062ff"/><stop offset="0.71" stop-color="#9216ea"/><stop offset="0.85" stop-color="#ee82ee"/><stop offset="1" stop-color="#ed2610"/></linearGradient>"##;

struct Row {
  label: &'static str,
  value: String,
  /// secondary detail, e.g. the date a best score was set
  detail: Option<String>,
  /// value color override for placement/percentile coloring
  color: Option<&'static str>,
  rainbow: bool,
}

fn row_fill<'a>(row: &'a Row, c: &'a EmbedColors) -> &'a str {
  if row.rainbow {
    "url(#rainbow)"
  } else if let Some(col) = row.color {
    col
  } else {
    c.text.as_str()
  }
}

fn commafy(n: usize) -> String {
  let s = n.to_string();
  let b = s.as_bytes();
  let mut out = String::with_capacity(s.len() + s.len() / 3);
  for (i, c) in b.iter().enumerate() {
    if i > 0 && (b.len() - i) % 3 == 0 {
      out.push(',');
    }
    out.push(*c as char);
  }
  out
}

/// a count with its global rank appended, e.g. "92 (#829)"; just the count when the rank is
/// unknown.
fn count_with_rank(count: usize, rank: Option<usize>) -> String {
  match rank {
    Some(r) => format!("{} (#{})", commafy(count), commafy(r)),
    None => commafy(count),
  }
}

fn esc(s: &str) -> String {
  s.replace('&', "&amp;")
    .replace('<', "&lt;")
    .replace('>', "&gt;")
    .replace('"', "&quot;")
}

/// `YYYYMMDD` -> `YYYY-MM-DD`
fn fmt_day_id(day_id: usize) -> String {
  format!(
    "{:04}-{:02}-{:02}",
    day_id / 10000,
    day_id / 100 % 100,
    day_id % 100
  )
}

/// trim a clock-rate like 1.30 -> "1.3", 1.00 -> "1"
fn fmt_rate(r: f64) -> String {
  let s = format!("{r:.2}");
  s.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// the user's modal mod combination, e.g. "DT 1.3x, HD" (clock-rate appended when present).
fn fmt_mod_combo(mods: &[Mod]) -> String {
  if mods.is_empty() {
    return "Nomod".to_owned();
  }
  mods
    .iter()
    .map(|m| {
      match m
        .settings
        .as_ref()
        .and_then(|s| s.get("speed_change"))
        .and_then(|v| v.as_f64())
      {
        Some(rate) => format!("{} {}x", m.acronym, fmt_rate(rate)),
        None => m.acronym.clone(),
      }
    })
    .collect::<Vec<_>>()
    .join(" ")
}

// rank palette mirroring the site's SS/S/A colors, shared by the placement/percentile/calendar
// coloring
const COLOR_SS: &str = "#CE1C9D";
const COLOR_S: &str = "#02B5C3";
const COLOR_A: &str = "#00AA00";

/// absolute-placement color, matching the site's `colorPlacement` (top 10 / top 50 / else text).
fn color_placement(rank: usize) -> Option<&'static str> {
  if rank == 0 {
    None
  } else if rank <= 10 {
    Some(COLOR_SS)
  } else if rank <= 50 {
    Some(COLOR_S)
  } else {
    None
  }
}

/// percentile color, matching the site's `colorPercentile` (top 1% / 10% / 50% / else text).
fn color_percentile(p: f32) -> Option<&'static str> {
  if p <= 1. {
    Some(COLOR_SS)
  } else if p <= 10. {
    Some(COLOR_S)
  } else if p <= 50. {
    Some(COLOR_A)
  } else {
    None
  }
}

/// resolve a stat to a renderable row; shared by the stats list and the hero panel.
fn resolve_stat(k: &StatKey, cfg: &EmbedConfig, s: &DailyChallengeUserStats) -> Row {
  let dash = || "—".to_string();
  let st = &s.streaks;
  let plain = |label: &'static str, value: String| Row {
    label,
    value,
    detail: None,
    color: None,
    rainbow: false,
  };
  match k {
    StatKey::Participation => {
      let full = cfg.rainbow_full_streak
        && s.total_challenge_count > 0
        && s.total_participation == s.total_challenge_count;
      Row {
        label: "Participation",
        value: format!(
          "{}/{}",
          commafy(s.total_participation),
          commafy(s.total_challenge_count)
        ),
        detail: None,
        color: None,
        rainbow: full,
      }
    },
    StatKey::CurrentDailyStreak => plain("Current Daily Streak", commafy(st.cur_daily_streak)),
    StatKey::BestDailyStreak => plain("Best Daily Streak", commafy(st.best_daily_streak)),
    StatKey::CurrentWeeklyStreak => plain("Current Weekly Streak", commafy(st.cur_weekly_streak)),
    StatKey::BestWeeklyStreak => plain("Best Weekly Streak", commafy(st.best_weekly_streak)),
    StatKey::GlobalRank => {
      let r = s.total_score_stats.total_score_rank;
      plain(
        "Global Rank",
        if r == 0 {
          dash()
        } else {
          format!("#{}", commafy(r))
        },
      )
    },
    StatKey::TotalScore => plain("Total Score", commafy(s.total_score_stats.total_score_sum)),
    StatKey::BestPlacementRank => match &s.best_placement_absolute {
      Some(b) => Row {
        label: "Best Placement",
        value: format!("#{} / {}", commafy(b.rank), commafy(b.total_rankings)),
        detail: None,
        color: color_placement(b.rank),
        rainbow: false,
      },
      None => plain("Best Placement", dash()),
    },
    StatKey::BestPlacementPercentile => match &s.best_placement_percentile {
      Some(b) => Row {
        label: "Best Percentile",
        value: format!("{:.2}%", b.percentile),
        detail: None,
        color: color_percentile(b.percentile),
        rainbow: false,
      },
      None => plain("Best Percentile", dash()),
    },
    StatKey::BestPlacementScore => match &s.best_placement_score {
      Some(b) => Row {
        label: "Best Score",
        value: commafy(b.score),
        detail: Some(fmt_day_id(b.day_id)),
        color: None,
        rainbow: false,
      },
      None => plain("Best Score", dash()),
    },
    StatKey::BestPlacementPp => match s.best_placement_pp.as_ref().filter(|b| b.pp.is_some()) {
      Some(b) => Row {
        label: "Highest PP",
        value: format!("{:.3}", b.pp.unwrap()),
        detail: Some(fmt_day_id(b.day_id)),
        color: None,
        rainbow: false,
      },
      None => plain("Highest PP", dash()),
    },
    StatKey::Top50Count => plain(
      "Top 50% Count",
      count_with_rank(s.top_50_percent_count, s.top_50_percent_rank),
    ),
    StatKey::Top10Count => plain(
      "Top 10% Count",
      count_with_rank(s.top_10_percent_count, s.top_10_percent_rank),
    ),
    StatKey::Top1Count => plain(
      "Top 1% Count",
      count_with_rank(s.top_1_percent_count, s.top_1_percent_rank),
    ),
    StatKey::FirstPlaceCount => plain(
      "1st Place Count",
      count_with_rank(s.first_place_count, s.first_place_rank),
    ),
    StatKey::Top1Streak => plain("Best Top 1% Streak", commafy(st.best_top_1_percent_streak)),
    StatKey::Top10Streak => plain(
      "Best Top 10% Streak",
      commafy(st.best_top_10_percent_streak),
    ),
    StatKey::Top50Streak => plain(
      "Best Top 50% Streak",
      commafy(st.best_top_50_percent_streak),
    ),
    StatKey::MostUsedMods => plain("Top Mod Combo", fmt_mod_combo(&s.most_used_mod_combo)),
  }
}

fn resolve_rows(cfg: &EmbedConfig, s: &DailyChallengeUserStats) -> Vec<Row> {
  cfg.stats.iter().map(|k| resolve_stat(k, cfg, s)).collect()
}

/// `percentiles`: day-of-month (1-based) -> percentile. `current_dom`: latest day that counts;
/// later days render as future.
struct CalendarData {
  year: u32,
  month: u32,
  current_dom: u32,
  percentiles: FxHashMap<u32, f32>,
}

const MONTHS: [&str; 12] = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

// calendar geometry: 7 columns of CELL separated by CAL_GAP
const CELL: f32 = 26.;
const CAL_GAP: f32 = 3.;
const CAL_W: f32 = 7. * CELL + 6. * CAL_GAP;
const CAL_INNER_PAD: f32 = 3.; // gap between the border and the cell grid
const MONTH_LABEL_H: f32 = 20.;
const CIRCLE_R: f32 = 10.; // played-day sticker radius (cell is 26)

// neutral grays derived from the background via `shade` so the calendar tracks the theme
const CAL_BORDER_SHADE: i32 = 29; // frame around the cell grid
const DAY_TILE_SHADE: i32 = 9; // played / missed day tile
const FUTURE_TILE_SHADE: i32 = 2; // not-yet-played day, fainter
const MISSED_STROKE_SHADE: i32 = 72; // diagonal strike on missed days

/// percentile -> played-day sticker color. Top buckets follow the site's `colorPercentile`; the
/// bottom-50% bucket uses a muted green (vs. the site's white) so it still reads as "completed".
fn bucket_color(p: f32) -> &'static str {
  if p <= 1. {
    COLOR_SS
  } else if p <= 10. {
    COLOR_S
  } else if p <= 50. {
    COLOR_A
  } else {
    "#36573e" // bottom-50% bucket
  }
}

/// parse "#rrggbb" -> (r, g, b); falls back to the default dark background on bad input.
fn parse_hex(s: &str) -> (u8, u8, u8) {
  let h = s.strip_prefix('#').unwrap_or(s);
  if h.len() == 6 {
    if let (Ok(r), Ok(g), Ok(b)) = (
      u8::from_str_radix(&h[0..2], 16),
      u8::from_str_radix(&h[2..4], 16),
      u8::from_str_radix(&h[4..6], 16),
    ) {
      return (r, g, b);
    }
  }
  (22, 22, 22)
}

/// nudge a color toward contrast by `amount` per channel: lighten a dark base, darken a light one.
fn shade(base: &str, amount: i32) -> String {
  let (r, g, b) = parse_hex(base);
  let lum = 0.299 * r as f32 + 0.587 * g as f32 + 0.114 * b as f32;
  let dir = if lum < 128. { 1 } else { -1 };
  let adj = |c: u8| ((c as i32 + dir * amount).clamp(0, 255)) as u8;
  format!("#{:02x}{:02x}{:02x}", adj(r), adj(g), adj(b))
}

/// current-month grid, Sunday-start. Played days = colored circle, missed = diagonal strike,
/// future = empty tile, adjacent-month = blank.
fn build_calendar_panel(c: &EmbedColors, cal: Option<&CalendarData>) -> (String, f32, f32) {
  let box_w = CAL_W + 2. * CAL_INNER_PAD;
  let panel_w = box_w;
  let Some(cal) = cal else {
    return (
      String::new(),
      panel_w,
      MONTH_LABEL_H + CELL + 2. * CAL_INNER_PAD,
    );
  };
  let cal_border = shade(&c.background, CAL_BORDER_SHADE);
  let day_tile = shade(&c.background, DAY_TILE_SHADE);
  let future_tile = shade(&c.background, FUTURE_TILE_SHADE);
  let missed_stroke = shade(&c.background, MISSED_STROKE_SHADE);
  let first = NaiveDate::from_ymd_opt(cal.year as i32, cal.month, 1).unwrap();
  let lead = first.weekday().num_days_from_sunday(); // leading blanks (Sun=0..Sat=6)
  let days_in_month = {
    let (ny, nm) = if cal.month == 12 {
      (cal.year + 1, 1)
    } else {
      (cal.year, cal.month + 1)
    };
    let next_first = NaiveDate::from_ymd_opt(ny as i32, nm, 1).unwrap();
    next_first.signed_duration_since(first).num_days() as u32
  };
  let n_rows = (lead + days_in_month + 6) / 7;
  let grid_h = n_rows as f32 * CELL + (n_rows as f32 - 1.) * CAL_GAP;
  let box_h = grid_h + 2. * CAL_INNER_PAD;
  let panel_h = MONTH_LABEL_H + box_h;

  let mut frag = format!(
    r#"<text x="{}" y="14" font-size="13" text-anchor="middle" fill="{}">{} {}</text><rect x="0.5" y="{}" width="{}" height="{}" fill="none" stroke="{cal_border}" stroke-width="1" shape-rendering="crispEdges"/>"#,
    panel_w / 2.,
    c.secondary_text,
    MONTHS[(cal.month - 1) as usize],
    cal.year,
    MONTH_LABEL_H + 0.5,
    box_w - 1.,
    box_h - 1.
  );
  let (ox, oy) = (CAL_INNER_PAD, MONTH_LABEL_H + CAL_INNER_PAD);
  for cell in 0..(n_rows * 7) {
    let x = ox + (cell % 7) as f32 * (CELL + CAL_GAP);
    let y = oy + (cell / 7) as f32 * (CELL + CAL_GAP);
    // blank padding for adjacent-month cells
    if cell < lead || cell - lead + 1 > days_in_month {
      continue;
    }
    let dom = cell - lead + 1;
    if dom > cal.current_dom {
      // future day
      frag.push_str(&format!(
        r#"<rect x="{x}" y="{y}" width="{CELL}" height="{CELL}" fill="{future_tile}" shape-rendering="crispEdges"/>"#
      ));
      continue;
    }
    // played/missed day tile
    frag.push_str(&format!(
      r#"<rect x="{x}" y="{y}" width="{CELL}" height="{CELL}" fill="{day_tile}" shape-rendering="crispEdges"/>"#
    ));
    let (cx, cy) = (x + CELL / 2., y + CELL / 2.);
    match cal.percentiles.get(&dom) {
      // played
      Some(p) => frag.push_str(&format!(
        r#"<circle cx="{cx}" cy="{cy}" r="{CIRCLE_R}" fill="{}"/>"#,
        bucket_color(*p)
      )),
      // missed
      None => frag.push_str(&format!(
        r#"<line x1="{}" y1="{}" x2="{}" y2="{}" stroke="{missed_stroke}" stroke-width="2.5" stroke-linecap="round"/>"#,
        x + 6.,
        y + 6.,
        x + CELL - 6.,
        y + CELL - 6.
      )),
    }
  }
  (frag, panel_w, panel_h)
}

const HIST_W: f32 = 240.;
const HIST_H: f32 = 152.;
const HIST_FILL: &str = "#24a6c7"; // site histogram accent
const HIST_M_TOP: f32 = 24.; // title strip
const HIST_M_LEFT: f32 = 36.; // y-axis labels
const HIST_M_BOTTOM: f32 = 18.; // x-axis labels
const HIST_M_RIGHT: f32 = 8.;

/// SI-suffixed score-axis tick (~d3 `.2s`): 0, 800k, 1.6M.
fn fmt_si(v: f32) -> String {
  let v = v.round();
  if v >= 1_000_000. {
    format!("{:.1}M", v / 1_000_000.)
  } else if v >= 1_000. {
    format!("{:.0}k", v / 1_000.)
  } else {
    format!("{:.0}", v)
  }
}

/// seconds-since-midnight -> hour label ("6 AM", "12 PM") for the time-of-day axis.
fn fmt_hour(secs: f32) -> String {
  let h = ((secs / 3600.).round() as i32).rem_euclid(24);
  let (h12, ampm) = match h {
    0 => (12, "AM"),
    1..=11 => (h, "AM"),
    12 => (12, "PM"),
    _ => (h - 12, "PM"),
  };
  format!("{h12} {ampm}")
}

/// bars scaled to the max bucket. `x_ticks` are domain fractions in [0,1]; `x_fmt` labels them.
fn build_histogram_panel(
  c: &EmbedColors,
  hist: &Histogram,
  title: &str,
  x_ticks: &[f32],
  x_fmt: fn(f32) -> String,
) -> (String, f32, f32) {
  let n = hist.buckets.len().max(1);
  let max = hist.buckets.iter().copied().max().unwrap_or(0).max(1);
  let x0 = HIST_M_LEFT;
  let y0 = HIST_M_TOP;
  let plot_w = HIST_W - HIST_M_LEFT - HIST_M_RIGHT;
  let plot_h = HIST_H - HIST_M_TOP - HIST_M_BOTTOM;
  let baseline = y0 + plot_h;

  let mut frag = format!(
    r#"<text x="{}" y="13" font-size="13" font-weight="600" text-anchor="middle" fill="{}">{}</text>"#,
    HIST_W / 2.,
    c.text,
    esc(title)
  );

  let bar_w = plot_w / n as f32;
  for (i, &b) in hist.buckets.iter().enumerate() {
    let h = b as f32 / max as f32 * plot_h;
    let x = x0 + i as f32 * bar_w;
    let w = (bar_w - 1.).max(0.5);
    frag.push_str(&format!(
      r#"<rect x="{x:.2}" y="{:.2}" width="{w:.2}" height="{h:.2}" fill="{HIST_FILL}"/>"#,
      baseline - h
    ));
  }

  // axis lines
  frag.push_str(&format!(
    r#"<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{baseline}" stroke="{}" stroke-width="1"/>"#,
    c.border
  ));
  frag.push_str(&format!(
    r#"<line x1="{x0}" y1="{baseline}" x2="{}" y2="{baseline}" stroke="{}" stroke-width="1"/>"#,
    x0 + plot_w,
    c.border
  ));

  // y-axis ticks: 0, half, max (counts)
  for k in 0..=2 {
    let v = max as f32 * k as f32 / 2.;
    let ty = baseline - v / max as f32 * plot_h;
    frag.push_str(&format!(
      r#"<line x1="{}" y1="{ty:.2}" x2="{x0}" y2="{ty:.2}" stroke="{}" stroke-width="1"/><text x="{}" y="{:.2}" font-size="9" text-anchor="end" fill="{}">{}</text>"#,
      x0 - 3.,
      c.border,
      x0 - 5.,
      ty + 3.,
      c.secondary_text,
      commafy(v.round() as usize)
    ));
  }

  // x-axis ticks at the requested domain fractions
  let last = x_ticks.len().saturating_sub(1);
  for (i, &f) in x_ticks.iter().enumerate() {
    let tx = x0 + f * plot_w;
    let v = hist.min + f * (hist.max - hist.min);
    let anchor = if i == 0 {
      "start"
    } else if i == last {
      "end"
    } else {
      "middle"
    };
    frag.push_str(&format!(
      r#"<line x1="{tx:.2}" y1="{baseline}" x2="{tx:.2}" y2="{}" stroke="{}" stroke-width="1"/><text x="{tx:.2}" y="{}" font-size="9" text-anchor="{anchor}" fill="{}">{}</text>"#,
      baseline + 3.,
      c.border,
      baseline + 13.,
      c.secondary_text,
      esc(&x_fmt(v))
    ));
  }

  (frag, HIST_W, HIST_H)
}

const HERO_W: f32 = 200.;
const HERO_H: f32 = 120.;

/// one scalar rendered large + centered (rainbow if eligible), label below.
fn build_hero_panel(
  cfg: &EmbedConfig,
  stats: &DailyChallengeUserStats,
  key: &StatKey,
) -> (String, f32, f32) {
  let row = resolve_stat(key, cfg, stats);
  let c = &cfg.colors;
  let fill = row_fill(&row, c);
  let cx = HERO_W / 2.;
  // auto-fit so long values don't overflow the panel
  let nchars = row.value.chars().count().max(1) as f32;
  let max_w = HERO_W - 12.;
  let approx = 0.62 * nchars; // ~glyph-width / font-size for IBM Plex SemiBold
  let font = if approx * 46. > max_w {
    (max_w / approx).max(14.)
  } else {
    46.
  };

  let frag = if let Some(detail) = &row.detail {
    let vy = HERO_H / 2. - 8.;
    format!(
      r#"<text x="{cx}" y="{vy}" font-size="{font}" font-weight="600" text-anchor="middle" fill="{fill}">{}</text><text x="{cx}" y="{}" font-size="16" text-anchor="middle" fill="{}">{}</text><text x="{cx}" y="{}" font-size="14" text-anchor="middle" fill="{}">{}</text>"#,
      esc(&row.value),
      vy + 22.,
      c.text,
      esc(detail),
      vy + 44.,
      c.secondary_text,
      esc(row.label)
    )
  } else {
    let vy = HERO_H / 2.;
    format!(
      r#"<text x="{cx}" y="{vy}" font-size="{font}" font-weight="600" text-anchor="middle" fill="{fill}">{}</text><text x="{cx}" y="{}" font-size="15" text-anchor="middle" fill="{}">{}</text>"#,
      esc(&row.value),
      vy + 30.,
      c.secondary_text,
      esc(row.label)
    )
  };
  (frag, HERO_W, HERO_H)
}

fn render_card(
  cfg: &EmbedConfig,
  username: &str,
  stats: &DailyChallengeUserStats,
  avatar: Option<&str>,
  calendar: Option<&CalendarData>,
) -> String {
  let rows = resolve_rows(cfg, stats);
  let c = &cfg.colors;
  let show_avatar = cfg.show_avatar && avatar.is_some();

  let panel: Option<(String, f32, f32)> = match &cfg.feature {
    Feature::None => None,
    Feature::Calendar => Some(build_calendar_panel(c, calendar)),
    Feature::ScoreHistogram => Some(build_histogram_panel(
      c,
      &stats.score_distribution,
      "Score Distribution",
      &[0., 0.5, 1.],
      fmt_si,
    )),
    Feature::TimeOfDayHistogram => Some(build_histogram_panel(
      c,
      &stats.time_of_day_distribution,
      "Time of Day Distribution",
      &[0., 0.25, 0.5, 0.75, 1.],
      fmt_hour,
    )),
    Feature::HeroStat(key) => Some(build_hero_panel(cfg, stats, key)),
  };

  // values right-align at the stats-column edge (panel present) or the card edge (single column)
  let right_x = if panel.is_some() {
    PAD_X + STATS_COL_W
  } else {
    W - PAD_X
  };
  let card_w = match &panel {
    Some((_, pw, _)) => PANEL_X + pw + PAD_X,
    None => W,
  };

  let mut defs = String::from(RAINBOW_DEF);
  let mut body = String::new();
  let mut y = PAD_Y;

  if cfg.show_header {
    let tx = if show_avatar { PAD_X + AV + 14. } else { PAD_X };
    if show_avatar {
      defs.push_str(&format!(
        r#"<clipPath id="av"><rect x="{PAD_X}" y="{y}" width="{AV}" height="{AV}"/></clipPath>"#
      ));
      body.push_str(&format!(
        r#"<image x="{PAD_X}" y="{y}" width="{AV}" height="{AV}" href="{}" clip-path="url(#av)" preserveAspectRatio="xMidYMid slice"/>"#,
        avatar.unwrap()
      ));
    }
    body.push_str(&format!(
      r#"<text x="{tx}" y="{}" font-size="22" font-weight="600" fill="{}">{}</text>"#,
      y + 24.,
      c.text,
      esc(username)
    ));
    body.push_str(&format!(
      r#"<text x="{tx}" y="{}" font-size="14" fill="{}">osu! Daily Challenge</text>"#,
      y + 45.,
      c.secondary_text
    ));
    y += AV;
    let dy = y + HEADER_GAP / 2.;
    body.push_str(&format!(
      r#"<line x1="{PAD_X}" y1="{dy}" x2="{right_x}" y2="{dy}" stroke="{}" stroke-width="1"/>"#,
      c.border
    ));
    y += HEADER_GAP;
  }

  for row in &rows {
    let by = y + ROW_H / 2. + 5.;
    body.push_str(&format!(
      r#"<text x="{PAD_X}" y="{by}" font-size="15" fill="{}">{}</text>"#,
      c.secondary_text,
      esc(row.label)
    ));
    let fill = row_fill(row, c);
    let value = match &row.detail {
      Some(d) => format!("{} - {}", row.value, d),
      None => row.value.clone(),
    };
    body.push_str(&format!(
      r#"<text x="{right_x}" y="{by}" font-size="15" font-weight="600" text-anchor="end" fill="{fill}">{}</text>"#,
      esc(&value)
    ));
    y += ROW_H;
  }

  let left_h = y - PAD_Y;
  let content_h = panel.as_ref().map_or(left_h, |(_, _, ph)| left_h.max(*ph));
  if let Some((frag, _, ph)) = &panel {
    // vertically center the panel within the content area
    let panel_y = PAD_Y + (content_h - ph) / 2.;
    body.push_str(&format!(
      r#"<g transform="translate({PANEL_X},{panel_y})">{frag}</g>"#
    ));
  }
  let height = 2. * PAD_Y + content_h;

  let font_style: &str = &FONT_FACE_STYLE;
  format!(
    r#"<svg xmlns="http://www.w3.org/2000/svg" width="{card_w}" height="{height}" viewBox="0 0 {card_w} {height}" font-family="{FONT_FAMILY}"><defs>{font_style}{defs}</defs><rect x="0.5" y="0.5" width="{}" height="{}" fill="{}" stroke="{}" stroke-width="1"/>{body}</svg>"#,
    card_w - 1.,
    height - 1.,
    c.background,
    c.border
  )
}

fn svg_to_png(svg: &str, scale: f32) -> Result<Vec<u8>, APIError> {
  let opts = usvg::Options {
    font_family: FONT_FAMILY.to_owned(),
    fontdb: FONTDB.clone(),
    ..Default::default()
  };
  let tree = usvg::Tree::from_str(svg, &opts).map_err(|err| {
    error!("failed to parse embed svg: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "failed to render embed".to_owned(),
    }
  })?;
  let size = tree.size();
  let w = ((size.width() * scale).round() as u32).max(1);
  let h = ((size.height() * scale).round() as u32).max(1);
  if w > MAX_PNG_DIMENSION || h > MAX_PNG_DIMENSION || u64::from(w) * u64::from(h) > MAX_PNG_PIXELS
  {
    return Err(APIError {
      status: StatusCode::BAD_REQUEST,
      message: "embed render size is too large".to_owned(),
    });
  }
  let mut pixmap = tiny_skia::Pixmap::new(w, h).ok_or_else(|| APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: "failed to alloc pixmap".to_owned(),
  })?;
  resvg::render(
    &tree,
    tiny_skia::Transform::from_scale(scale, scale),
    &mut pixmap.as_mut(),
  );
  pixmap.encode_png().map_err(|err| {
    error!("failed to encode embed png: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "failed to encode embed".to_owned(),
    }
  })
}

async fn fetch_avatar(user_id: u64) -> Option<String> {
  if let Some(cached) = AVATAR_CACHE.get(&user_id) {
    return cached;
  }
  let uri = (|| async {
    let resp = HTTP
      .get(format!("https://a.ppy.sh/{user_id}"))
      .send()
      .await
      .ok()?;
    if !resp.status().is_success() {
      return None;
    }
    let bytes = resp.bytes().await.ok()?;
    if bytes.len() as u64 > MAX_AVATAR_BYTES {
      return None;
    }
    let mime = if bytes.starts_with(&[0xFF, 0xD8]) {
      "image/jpeg"
    } else if bytes.starts_with(&[0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1A, b'\n']) {
      "image/png"
    } else {
      return None;
    };
    Some(format!(
      "data:{mime};base64,{}",
      base64::engine::general_purpose::STANDARD.encode(&bytes)
    ))
  })()
  .await;
  AVATAR_CACHE.insert(user_id, uri.clone());
  uri
}

async fn resolve_username(user_id: u64) -> String {
  if let Some(name) = USERNAME_CACHE.get(&user_id) {
    return name;
  }
  let name = if let Ok(Some(name)) =
    sqlx::query_scalar!("SELECT username FROM users WHERE osu_id = ?", user_id)
      .fetch_optional(db_pool())
      .await
  {
    name
  } else {
    crate::osu_api::fetch_username(user_id)
      .await
      .ok()
      .flatten()
      .unwrap_or_else(|| user_id.to_string())
  };
  USERNAME_CACHE.insert(user_id, name.clone());
  name
}

async fn cached_user_stats(
  user_id: u64,
  day_id: usize,
) -> Result<Arc<DailyChallengeUserStats>, APIError> {
  if let Some(stats) = USER_STATS_CACHE.get(&(user_id, day_id)) {
    return Ok(stats);
  }
  let stats = Arc::new(compute_user_daily_challenge_stats(user_id as usize).await?);
  USER_STATS_CACHE.insert((user_id, day_id), stats.clone());
  Ok(stats)
}

async fn render_svg(user_id: u64, day_id: usize, cfg: &EmbedConfig) -> Result<String, APIError> {
  // independent; run concurrently so a cold avatar fetch overlaps the DB work
  let avatar_fut = async {
    if cfg.show_header && cfg.show_avatar {
      fetch_avatar(user_id).await
    } else {
      None
    }
  };
  let calendar_fut = async {
    if cfg.feature == Feature::Calendar {
      let year = (day_id / 10000) as u32;
      let month = (day_id / 100 % 100) as u32;
      let current_dom = (day_id % 100) as u32;
      let percentiles = user_month_percentiles(user_id, year, month).await?;
      Ok(Some(CalendarData {
        year,
        month,
        current_dom,
        percentiles,
      }))
    } else {
      Ok::<_, APIError>(None)
    }
  };
  let (stats, username, avatar, calendar) = tokio::join!(
    cached_user_stats(user_id, day_id),
    resolve_username(user_id),
    avatar_fut,
    calendar_fut
  );
  let stats = stats?;
  let calendar = calendar?;
  Ok(render_card(
    cfg,
    &username,
    &stats,
    avatar.as_deref(),
    calendar.as_ref(),
  ))
}

fn cache_file(user_id: u64, hash: &str, day_id: usize, ext: &str) -> PathBuf {
  PathBuf::from(&SETTINGS.get().unwrap().daily_challenge.embed_cache_dir)
    .join("embeds")
    .join(user_id.to_string())
    .join(format!("{hash}-v{RENDER_VERSION}-{day_id}.{ext}"))
}

/// background sweep cadence and TTL. Active embeds re-render daily (and `prune_siblings` collapses
/// them to one file per (hash, ext)); anything older than this is an embed nobody is fetching.
const CACHE_PRUNE_INTERVAL: Duration = Duration::from_secs(6 * 3600);
const MAX_CACHE_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

pub(crate) fn spawn_cache_pruner() {
  tokio::spawn(async {
    loop {
      let _ = tokio::task::spawn_blocking(prune_embed_cache).await;
      tokio::time::sleep(CACHE_PRUNE_INTERVAL).await;
    }
  });
}

/// walk `<embed_cache_dir>/embeds/<user>/` and drop files whose mtime is past `MAX_CACHE_AGE`. mtime
/// is a clean liveness signal here because cache hits don't rewrite the file — only a fresh render
/// bumps it. Removes per-user dirs left empty by the sweep.
fn prune_embed_cache() {
  let root = PathBuf::from(&SETTINGS.get().unwrap().daily_challenge.embed_cache_dir).join("embeds");
  let now = std::time::SystemTime::now();
  let Ok(users) = std::fs::read_dir(&root) else {
    return;
  };
  let mut deleted = 0usize;
  for user_entry in users.flatten() {
    let user_path = user_entry.path();
    if !user_path.is_dir() {
      continue;
    }
    let Ok(files) = std::fs::read_dir(&user_path) else {
      continue;
    };
    let mut remaining = 0usize;
    for file_entry in files.flatten() {
      let stale = file_entry
        .metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| now.duration_since(t).ok())
        .is_some_and(|age| age > MAX_CACHE_AGE);
      if stale && std::fs::remove_file(file_entry.path()).is_ok() {
        deleted += 1;
      } else {
        remaining += 1;
      }
    }
    if remaining == 0 {
      let _ = std::fs::remove_dir(&user_path);
    }
  }
  if deleted > 0 {
    info!("pruned {deleted} stale embed cache file(s)");
  }
}

/// drop this hash's renders from previous days (same ext only, so .svg and .png coexist)
fn prune_siblings(path: &std::path::Path, hash: &str, ext: &str, keep: &str) {
  let Some(dir) = path.parent() else { return };
  let prefix = format!("{hash}-");
  let suffix = format!(".{ext}");
  let Ok(entries) = std::fs::read_dir(dir) else {
    return;
  };
  for entry in entries.flatten() {
    let name = entry.file_name();
    let name = name.to_string_lossy();
    if name.starts_with(&prefix) && name.ends_with(&suffix) && name != keep {
      let _ = std::fs::remove_file(entry.path());
    }
  }
}

fn db_err(err: sqlx::Error) -> APIError {
  error!("embed db error: {err}");
  APIError {
    status: StatusCode::INTERNAL_SERVER_ERROR,
    message: "database error".to_owned(),
  }
}

pub(crate) async fn create_embed(
  Json(req): Json<EmbedRequest>,
) -> Result<Json<serde_json::Value>, APIError> {
  validate_config(&req.config)?;
  let user_id = db_user_id(req.user_id)?;
  let hash = config_hash(&req.config);
  let config = serde_json::to_string(&req.config).unwrap();
  sqlx::query!(
    "INSERT IGNORE INTO daily_challenge_embed (hash, user_id, config) VALUES (?, ?, ?)",
    hash,
    user_id,
    config
  )
  .execute(db_pool())
  .await
  .map_err(db_err)?;
  Ok(Json(serde_json::json!({ "hash": hash })))
}

pub(crate) async fn preview_embed(Json(req): Json<EmbedRequest>) -> Result<Response, APIError> {
  validate_config(&req.config)?;
  let _ = db_user_id(req.user_id)?;
  let day_id = latest_day_id().await?;
  let svg = render_svg(req.user_id, day_id, &req.config).await?;
  let mut headers = HeaderMap::new();
  headers.insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));
  headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
  Ok((headers, svg).into_response())
}

pub(crate) async fn get_embed(
  Path((user_id, file)): Path<(u64, String)>,
  headers: HeaderMap,
) -> Result<Response, APIError> {
  let (hash, ext) = file.rsplit_once('.').ok_or_else(|| APIError {
    status: StatusCode::NOT_FOUND,
    message: "missing extension".to_owned(),
  })?;
  if !validate_hash(hash) {
    return Err(APIError {
      status: StatusCode::NOT_FOUND,
      message: "unknown embed".to_owned(),
    });
  }
  if ext != "json" && ext != "png" && ext != "svg" {
    return Err(APIError {
      status: StatusCode::NOT_FOUND,
      message: "unsupported extension".to_owned(),
    });
  }
  let user_id_db = db_user_id(user_id)?;

  let config = sqlx::query_scalar!(
    "SELECT config AS `config: serde_json::Value` FROM daily_challenge_embed WHERE hash = ? AND \
     user_id = ?",
    hash,
    user_id_db
  )
  .fetch_optional(db_pool())
  .await
  .map_err(db_err)?;
  let Some(config) = config else {
    return Err(APIError {
      status: StatusCode::NOT_FOUND,
      message: "unknown embed".to_owned(),
    });
  };
  let config: EmbedConfig = serde_json::from_value(config).map_err(|err| {
    error!("failed to parse stored embed config: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "corrupt embed config".to_owned(),
    }
  })?;
  validate_config(&config)?;

  if ext == "json" {
    let username = resolve_username(user_id).await;
    let title = format!("{username} — osu! Daily Challenge");
    return Ok(
      Json(serde_json::json!({ "config": config, "username": username, "title": title }))
        .into_response(),
    );
  }

  let day_id = latest_day_id().await?;
  let etag = format!("\"{hash}-v{RENDER_VERSION}-{day_id}\"");
  let content_type = if ext == "svg" {
    "image/svg+xml"
  } else {
    "image/png"
  };

  let mut resp_headers = HeaderMap::new();
  resp_headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
  resp_headers.insert(CACHE_CONTROL, HeaderValue::from_static(CACHE_CONTROL_VALUE));
  resp_headers.insert(ETAG, HeaderValue::from_str(&etag).unwrap());

  if headers.get(IF_NONE_MATCH).and_then(|v| v.to_str().ok()) == Some(etag.as_str()) {
    return Ok((StatusCode::NOT_MODIFIED, resp_headers).into_response());
  }

  let path = cache_file(user_id, hash, day_id, ext);
  let bytes = match std::fs::read(&path) {
    Ok(bytes) => bytes,
    Err(_) => {
      let svg = render_svg(user_id, day_id, &config).await?;
      let bytes = if ext == "svg" {
        svg.into_bytes()
      } else {
        svg_to_png(&svg, config.scale.unwrap_or(2.))?
      };
      if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
      }
      let keep = path.file_name().unwrap().to_string_lossy().into_owned();
      prune_siblings(&path, hash, ext, &keep);
      if let Err(err) = std::fs::write(&path, &bytes) {
        error!("failed to write embed cache file {path:?}: {err}");
      }
      bytes
    },
  };

  Ok((resp_headers, bytes).into_response())
}
