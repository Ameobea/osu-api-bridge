use foundations::{settings::settings, telemetry::settings::TelemetrySettings};
use serde_default_utils::*;

#[cfg(feature = "sql")]
#[settings]
pub struct SqlSettings {
  pub db_url: String,
}

#[cfg(feature = "daily_challenge")]
#[settings]
pub struct DailyChallengeSettings {
  pub admin_token: String,
  /// dir for cached rendered embeds
  #[serde(default = "default_embed_cache_dir")]
  pub embed_cache_dir: String,
}

#[cfg(feature = "daily_challenge")]
fn default_embed_cache_dir() -> String { "./embed_cache".to_owned() }

#[cfg(feature = "simulate_play")]
#[settings]
pub struct DiffcalcSettings {
  /// Loopback URL for the private canonical difficulty/PP sidecar.
  #[serde(default = "default_diffcalc_url")]
  pub url: String,
  /// Shared secret sent in X-Diffcalc-Key.
  #[serde(default)]
  pub api_key: String,
  /// Must be no larger than the sidecar's configured MaxBatchSize.
  #[serde(default = "default_usize::<256>")]
  pub max_batch_size: usize,
  /// Timeout for each internal HTTP request to the sidecar.
  #[serde(default = "default_u64::<20_000>")]
  pub request_timeout_ms: u64,
}

#[cfg(feature = "simulate_play")]
fn default_diffcalc_url() -> String { "http://127.0.0.1:4512".to_owned() }

#[cfg(feature = "simulate_play")]
fn default_diffcalc_settings() -> DiffcalcSettings {
  DiffcalcSettings {
    url: default_diffcalc_url(),
    api_key: String::new(),
    max_batch_size: 256,
    request_timeout_ms: 20_000,
  }
}

#[settings]
pub struct ServerSettings {
  /// Telemetry settings.
  pub telemetry: TelemetrySettings,

  /// Port that the HTTP server will listen on.
  #[serde(default = "default_u16::<4510>")]
  pub port: u16,
  /// Osu! OAuth client ID
  pub osu_client_id: u32,
  /// Osu! OAuth client secret
  pub osu_client_secret: String,
  /// Salt used for analytics event verification.
  pub analytics_salt: String,
  #[cfg(feature = "sql")]
  pub sql: SqlSettings,
  #[cfg(feature = "simulate_play")]
  #[serde(default = "default_diffcalc_settings")]
  pub diffcalc: DiffcalcSettings,
  #[cfg(feature = "daily_challenge")]
  pub daily_challenge: DailyChallengeSettings,
}
