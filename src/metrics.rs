use foundations::telemetry::metrics::{metrics, Counter, HistogramBuilder, TimeHistogram};

#[metrics]
pub mod http_server {
  /// Number of HTTP requests
  pub fn requests_total(endpoint_name: &'static str) -> Counter;

  /// Number of successful HTTP requests
  pub fn requests_success_total(endpoint_name: &'static str) -> Counter;

  /// Number of failed requests
  pub fn requests_failed_total(endpoint_name: &'static str) -> Counter;

  /// End-to-end duration of instrumented HTTP handlers.
  #[ctor = HistogramBuilder {
    buckets: &[
      0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.4, 0.6, 0.8, 1.0, 1.25,
      1.5, 1.75, 2.0, 2.5, 5.0, 10.0, 20.0,
    ],
  }]
  pub fn request_duration_seconds(endpoint_name: &'static str) -> TimeHistogram;

  /// Number of requests made to the osu! API
  pub fn osu_api_requests_total(endpoint_name: &'static str) -> Counter;

  /// Number of failed requests made to the osu! API
  pub fn osu_api_requests_failed_total(endpoint_name: &'static str, status_code: u16) -> Counter;

  /// Distribution of response times for the osu! API
  #[ctor = HistogramBuilder {
    buckets: &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
  }]
  pub fn osu_api_response_time_seconds(endpoint_name: &'static str) -> TimeHistogram;

  /// Number of requests made to refresh the OAuth token
  pub fn oauth_refresh_requests_total() -> Counter;

  /// Number of failed requests made to refresh the OAuth token
  pub fn oauth_refresh_requests_failed_total() -> Counter;

  /// Distribution of response times for fetching the OAuth token
  #[ctor = HistogramBuilder {
    buckets: &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
  }]
  pub fn oauth_refresh_response_time_seconds() -> TimeHistogram;

  /// Distribution of compute beatmap difficulties durations
  #[ctor = HistogramBuilder {
    buckets: &[0.00001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
  }]
  pub fn compute_beatmap_difficulties_duration() -> TimeHistogram;

  /// Number of requests made to the private canonical diffcalc sidecar.
  pub fn diffcalc_requests_total(operation: &'static str, status: &'static str) -> Counter;

  /// Distribution of response times from the private diffcalc sidecar.
  #[ctor = HistogramBuilder {
    buckets: &[
      0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.4, 0.6, 0.8, 1.0, 1.25,
      1.5, 1.75, 2.0, 2.5, 5.0, 10.0, 20.0,
    ],
  }]
  pub fn diffcalc_response_time_seconds(operation: &'static str) -> TimeHistogram;

  /// Number of public simulation requests rejected by the edge guard.
  pub fn simulation_rejections_total(reason: &'static str) -> Counter;

  /// Number of analytics events
  pub fn analytics_events_total(project: String, category: String, subcategory: String) -> Counter;
}
