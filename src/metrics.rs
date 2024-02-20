use foundations::telemetry::metrics::{metrics, Counter};
use std::sync::Arc;

#[metrics]
pub(crate) mod http_server {
  /// Number of HTTP requests.
  pub fn requests_total(endpoint_name: &Arc<String>) -> Counter;

  /// Number of successful HTTP requests.
  pub fn requests_success_total(endpoint_name: &Arc<String>) -> Counter;

  /// Number of failed requests.
  pub fn requests_failed_total(endpoint_name: &Arc<String>, status_code: u16) -> Counter;

  /// Number of requests made to the osu! API.
  pub fn osu_api_requests_total(endpoint_name: &'static str) -> Counter;

  /// Number of failed requests made to the osu! API.
  pub fn osu_api_requests_failed_total(endpoint_name: &'static str, status_code: u16) -> Counter;

  /// Number of requests made to refresh the OAuth token.
  pub fn oauth_refresh_requests_total() -> Counter;

  /// Number of failed requests made to refresh the OAuth token.
  pub fn oauth_refresh_requests_failed_total() -> Counter;
}
