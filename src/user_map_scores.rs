use std::io::Write;

use chrono::{NaiveDateTime, Utc};
use serde_json::Value;

use crate::db::db_pool;

struct ScoreRow {
  score_id: u64,
  ruleset: u8,
  mods: String,
  pp: Option<f64>,
  accuracy: f64,
  max_combo: i64,
  total_score: i64,
  rank: String,
  passed: bool,
  is_lazer: bool,
  legacy_score_id: Option<u64>,
  ended_at: NaiveDateTime,
  raw_gz: Vec<u8>,
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
  let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
  enc.write_all(bytes).unwrap();
  enc.finish().unwrap()
}

// Mirrors the collector's `Play::from_json` (osu-beatmap-embeddings sampler/src/collect.rs) so both
// writers produce identical rows.
fn parse(j: &Value) -> Option<ScoreRow> {
  let mut acronyms: Vec<&str> = j["mods"]
    .as_array()?
    .iter()
    .filter_map(|m| m["acronym"].as_str())
    .collect();
  acronyms.sort_unstable();
  let mut slim = j.clone();
  if let Some(o) = slim.as_object_mut() {
    for k in ["user", "beatmapset", "current_user_attributes"] {
      o.remove(k);
    }
  }
  Some(ScoreRow {
    score_id: j["id"].as_u64()?,
    ruleset: j["ruleset_id"].as_u64()? as u8,
    mods: acronyms.concat(),
    pp: j["pp"].as_f64(),
    accuracy: j["accuracy"].as_f64().unwrap_or(0.),
    max_combo: j["max_combo"].as_i64().unwrap_or(0),
    total_score: j["total_score"].as_i64().unwrap_or(0),
    rank: j["rank"].as_str().unwrap_or("").to_owned(),
    passed: j["passed"].as_bool().unwrap_or(false),
    is_lazer: j["build_id"].is_number(),
    legacy_score_id: j["legacy_score_id"].as_u64().filter(|&id| id > 0),
    ended_at: chrono::DateTime::parse_from_rfc3339(j["ended_at"].as_str()?)
      .ok()?
      .naive_utc(),
    raw_gz: gzip(&serde_json::to_vec(&slim).unwrap()),
  })
}

async fn store_inner(user_id: u64, beatmap_id: u64, scores: &[Value]) -> Result<(), sqlx::Error> {
  let now = Utc::now().naive_utc();
  let rows: Vec<ScoreRow> = scores.iter().filter_map(parse).collect();
  let mut tx = db_pool().begin().await?;
  for r in &rows {
    sqlx::query(
      "INSERT INTO user_map_scores (score_id, user_id, beatmap_id, ruleset, mods, pp, accuracy, \
       max_combo, total_score, `rank`, passed, is_lazer, legacy_score_id, ended_at, source, \
       fetched_at, raw_json_gz) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'bridge', ?, ?) \
       ON DUPLICATE KEY UPDATE pp = VALUES(pp), source = VALUES(source), fetched_at = \
       VALUES(fetched_at), raw_json_gz = VALUES(raw_json_gz)",
    )
    .bind(r.score_id)
    .bind(user_id)
    .bind(beatmap_id)
    .bind(r.ruleset)
    .bind(&r.mods)
    .bind(r.pp)
    .bind(r.accuracy)
    .bind(r.max_combo)
    .bind(r.total_score)
    .bind(&r.rank)
    .bind(r.passed)
    .bind(r.is_lazer)
    .bind(r.legacy_score_id)
    .bind(r.ended_at)
    .bind(now)
    .bind(&r.raw_gz)
    .execute(&mut *tx)
    .await?;
  }
  // Same rules as the collector: a preserved score implies a pass, none a non-pass unless an
  // earlier check already saw one.
  sqlx::query(
    "INSERT INTO user_map_score_checks (user_id, beatmap_id, passed, passed_checked_at, \
     scores_fetched_at, n_scores) VALUES (?, ?, ? > 0, ?, ?, ?) ON DUPLICATE KEY UPDATE passed = \
     IF(VALUES(n_scores) > 0, 1, COALESCE(passed, 0)), passed_checked_at = IF(VALUES(n_scores) > 0 \
     OR passed_checked_at IS NULL, VALUES(passed_checked_at), passed_checked_at), \
     scores_fetched_at = VALUES(scores_fetched_at), n_scores = VALUES(n_scores)",
  )
  .bind(user_id)
  .bind(beatmap_id)
  .bind(rows.len() as i64)
  .bind(now)
  .bind(now)
  .bind(rows.len() as i64)
  .execute(&mut *tx)
  .await?;
  tx.commit().await
}

/// Records an osu!standard `/scores/users/{u}/all` result in the background; failures are logged
/// and never affect the response.
pub fn store(user_id: u64, beatmap_id: u64, scores: Vec<Value>) {
  tokio::spawn(async move {
    if let Err(err) = store_inner(user_id, beatmap_id, &scores).await {
      error!("Failed to store user_map_scores for user {user_id} beatmap {beatmap_id}: {err}");
    }
  });
}
