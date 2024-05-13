use foundations::{settings::settings, telemetry::settings::TelemetrySettings};
use serde_default_utils::*;

#[cfg(feature = "sql")]
#[settings]
pub struct SqlSettings {
  pub db_url: String,
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
  #[cfg(feature = "sql")]
  pub sql: SqlSettings,
}
