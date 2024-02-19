use foundations::{
  settings::{settings, to_yaml_string},
  telemetry::settings::TelemetrySettings,
  BootstrapResult,
};
use serde_default_utils::*;

#[settings]
pub(crate) struct ServerSettings {
  /// Telemetry settings.
  pub(crate) telemetry: TelemetrySettings,

  /// Port that the HTTP server will listen on.
  #[serde(default = "default_u16::<4510>")]
  pub port: u16,
  /// Osu! OAuth client ID
  pub osu_client_id: u32,
  /// Osu! OAuth client secret
  pub osu_client_secret: String,
}

impl ServerSettings {
  pub fn write_default_settings() -> BootstrapResult<()> {
    let settings = Self::default();
    let yaml_string = to_yaml_string(&settings)?;

    std::fs::write("default-config.yml", yaml_string)?;
    Ok(())
  }
}
