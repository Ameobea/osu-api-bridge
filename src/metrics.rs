use foundations::telemetry::metrics::{metrics, Counter, Gauge, HistogramBuilder, TimeHistogram};

#[metrics]
pub mod http_server {
  /// Number of HTTP requests.
  pub fn requests_total(endpoint_name: &'static str) -> Counter;

  /// Number of successful HTTP requests.
  pub fn requests_success_total(endpoint_name: &'static str) -> Counter;

  /// Number of failed requests.
  pub fn requests_failed_total(endpoint_name: &'static str) -> Counter;

  /// Number of requests made to the osu! API.
  pub fn osu_api_requests_total(endpoint_name: &'static str) -> Counter;

  /// Number of failed requests made to the osu! API.
  pub fn osu_api_requests_failed_total(endpoint_name: &'static str, status_code: u16) -> Counter;

  /// Distribution of response times for the osu! API.
  #[ctor = HistogramBuilder {
    buckets: &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
  }]
  pub fn osu_api_response_time_seconds(endpoint_name: &'static str) -> TimeHistogram;

  /// Number of requests made to refresh the OAuth token.
  pub fn oauth_refresh_requests_total() -> Counter;

  /// Number of failed requests made to refresh the OAuth token.
  pub fn oauth_refresh_requests_failed_total() -> Counter;

  /// Distribution of response times for fetching the OAuth token.
  #[ctor = HistogramBuilder {
    buckets: &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
  }]
  pub fn oauth_refresh_response_time_seconds() -> TimeHistogram;

  /// Distribution of response times for downloading beatmaps.
  #[ctor = HistogramBuilder {
    buckets: &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
  }]
  pub fn beatmap_download_response_time_seconds() -> TimeHistogram;

  /// Number of bytes currently cached in the beatmap cache.
  pub fn beatmap_cache_bytes() -> Gauge;

  /// Number of analytics events.
  pub fn analytics_events_total(category: String, subcategory: String) -> Counter;
}
